"""Birth place resolution services with cache and provider abstraction."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from unicodedata import normalize

from geopy.exc import GeocoderServiceError, GeocoderTimedOut, GeocoderUnavailable
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
RUNTIME_CACHE_DIR = BASE_DIR / ".runtime_cache"
GEOCODE_CACHE_FILE = RUNTIME_CACHE_DIR / "geocode_cache.json"
tf = TimezoneFinder()


class BirthPlaceResolutionError(ValueError):
    """Raised when a birth place cannot be resolved safely."""

    def __init__(self, message: str, *, code: str = "resolution_failed", details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "details": self.details}


class GeocodingProvider:
    """Base abstraction for swappable geocoding providers."""

    provider_name = "unknown"

    def search(self, place_text: str) -> list[Any]:
        raise NotImplementedError


class NominatimGeocodingProvider(GeocodingProvider):
    """Nominatim-backed provider with district-aware address detail search."""

    provider_name = "nominatim"

    def __init__(self) -> None:
        self.client = Nominatim(user_agent="astro_yuzu_v3_birthplace", timeout=8, proxies={})

    def search(self, place_text: str) -> list[Any]:
        try:
            results = self.client.geocode(
                place_text,
                exactly_one=False,
                addressdetails=True,
                language="en",
            )
        except (GeocoderUnavailable, GeocoderTimedOut, GeocoderServiceError) as exc:
            raise BirthPlaceResolutionError(
                "Birth place could not be resolved right now.",
                code="provider_unavailable",
                details={"provider": self.provider_name},
            ) from exc
        return list(results or [])


_PROVIDER: GeocodingProvider = NominatimGeocodingProvider()
_MEMORY_CACHE: dict[str, dict[str, Any]] = {}


def _normalize_place_key(value: str) -> str:
    normalized = normalize("NFKD", (value or "").strip().lower()).encode("ascii", "ignore").decode("ascii")
    return " ".join(normalized.replace("ı", "i").split())


def _ensure_cache_dir() -> None:
    RUNTIME_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _load_cache() -> dict[str, dict[str, Any]]:
    _ensure_cache_dir()
    if not GEOCODE_CACHE_FILE.exists():
        return {}
    try:
        return json.loads(GEOCODE_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Geocode cache file could not be read; using empty cache.")
        return {}


def _save_cache(cache: dict[str, dict[str, Any]]) -> None:
    _ensure_cache_dir()
    GEOCODE_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_cached_resolution(cache_key: str) -> dict[str, Any] | None:
    if cache_key in _MEMORY_CACHE:
        cached = dict(_MEMORY_CACHE[cache_key])
        cached["cache_hit"] = True
        cached["location_source"] = "cache"
        return cached
    cache = _load_cache()
    cached = cache.get(cache_key)
    if cached:
        _MEMORY_CACHE[cache_key] = dict(cached)
        cached_payload = dict(cached)
        cached_payload["cache_hit"] = True
        cached_payload["location_source"] = "cache"
        return cached_payload
    return None


def _set_cached_resolution(cache_key: str, payload: dict[str, Any]) -> None:
    _MEMORY_CACHE[cache_key] = dict(payload)
    cache = _load_cache()
    cache[cache_key] = dict(payload)
    _save_cache(cache)


def _get_cached_search(cache_key: str) -> list[dict[str, Any]] | None:
    full_key = f"search::{cache_key}"
    if full_key in _MEMORY_CACHE:
        return [dict(item) for item in _MEMORY_CACHE[full_key]["results"]]
    cache = _load_cache()
    cached = cache.get(full_key)
    if cached:
        _MEMORY_CACHE[full_key] = {"results": [dict(item) for item in cached.get("results", [])]}
        return [dict(item) for item in cached.get("results", [])]
    return None


def _set_cached_search(cache_key: str, results: list[dict[str, Any]]) -> None:
    full_key = f"search::{cache_key}"
    payload = {"results": [dict(item) for item in results]}
    _MEMORY_CACHE[full_key] = payload
    cache = _load_cache()
    cache[full_key] = payload
    _save_cache(cache)


def _candidate_score(raw_input: str, candidate: Any) -> float:
    normalized_input = _normalize_place_key(raw_input)
    tokens = [token for token in normalized_input.split(",") if token.strip()]
    display_name = _normalize_place_key(getattr(candidate, "address", "") or candidate.raw.get("display_name", ""))
    address = {key: _normalize_place_key(value) for key, value in (candidate.raw.get("address") or {}).items()}
    category = _normalize_place_key(candidate.raw.get("type", ""))

    score = 0.0
    preferred_keys = ["suburb", "city_district", "town", "city", "province", "state", "country", "county"]
    joined_address = " ".join(address.get(key, "") for key in preferred_keys)
    for index, token in enumerate(tokens):
        if token in display_name:
            score += 3.0 if index == 0 else 2.0
        if token and token in joined_address:
            score += 2.5 if index == 0 else 1.5
    if category in {"suburb", "city_district", "town", "city"}:
        score += 1.0
    elif category in {"county", "state"}:
        score += 0.4
    return score


def _select_best_candidate(place_text: str, candidates: list[Any]) -> Any:
    if not candidates:
        raise BirthPlaceResolutionError(
            "Birth place could not be resolved.",
            code="no_match",
            details={"raw_input": place_text},
        )

    scored = sorted(
        [(_candidate_score(place_text, candidate), candidate) for candidate in candidates],
        key=lambda item: item[0],
        reverse=True,
    )
    best_score, best_candidate = scored[0]
    if best_score < 2.0:
        raise BirthPlaceResolutionError(
            "Birth place is too ambiguous to resolve safely.",
            code="ambiguous_place",
            details={
                "raw_input": place_text,
                "candidates": [getattr(candidate, "address", "") for _, candidate in scored[:3]],
            },
        )

    if len(scored) > 1:
        second_score, second_candidate = scored[1]
        lat_gap = abs(float(best_candidate.latitude) - float(second_candidate.latitude))
        lon_gap = abs(float(best_candidate.longitude) - float(second_candidate.longitude))
        if (best_score - second_score) <= 0.5 and (lat_gap > 0.15 or lon_gap > 0.15):
            raise BirthPlaceResolutionError(
                "Birth place returned multiple strong matches. Please enter district, city, and country more clearly.",
                code="ambiguous_place",
                details={
                    "raw_input": place_text,
                    "candidates": [getattr(candidate, "address", "") for _, candidate in scored[:3]],
                },
            )
    return best_candidate


def _resolve_timezone(latitude: float, longitude: float) -> str:
    timezone_name = tf.timezone_at(lng=longitude, lat=latitude) or tf.certain_timezone_at(lng=longitude, lat=latitude)
    if not timezone_name:
        raise BirthPlaceResolutionError(
            "Timezone could not be resolved from coordinates.",
            code="timezone_unresolved",
            details={"latitude": latitude, "longitude": longitude},
        )
    return timezone_name


def _serialize_candidate(raw_input: str, candidate: Any) -> dict[str, Any]:
    latitude = round(float(candidate.latitude), 6)
    longitude = round(float(candidate.longitude), 6)
    timezone_name = _resolve_timezone(latitude, longitude)
    raw_address = candidate.raw.get("display_name", getattr(candidate, "address", raw_input))
    confidence = round(min(_candidate_score(raw_input, candidate) / 10.0, 1.0), 3)
    display_parts = []
    address = candidate.raw.get("address") or {}
    for key in ("suburb", "city_district", "town", "city", "state", "country"):
        value = address.get(key)
        if value and value not in display_parts:
            display_parts.append(value)
    display_name = ", ".join(display_parts) if display_parts else raw_address
    return {
        "display_name": display_name,
        "normalized_place": raw_address,
        "latitude": latitude,
        "longitude": longitude,
        "timezone": timezone_name,
        "provider": str(getattr(_PROVIDER, "provider_name", "unknown")),
        "confidence": confidence,
        "raw_input": raw_input,
        "cache_hit": False,
        "location_source": f"provider:{getattr(_PROVIDER, 'provider_name', 'unknown')}",
    }


def resolve_birth_place(place_text: str) -> dict[str, Any]:
    """Resolve user-entered birth place text into normalized geographic data."""
    raw_input = (place_text or "").strip()
    if not raw_input:
        raise BirthPlaceResolutionError("Birth place is required.", code="missing_place")

    cache_key = _normalize_place_key(raw_input)
    cached = _get_cached_resolution(cache_key)
    if cached:
        return cached

    candidates = _PROVIDER.search(raw_input)
    selected = _select_best_candidate(raw_input, candidates)
    payload = _serialize_candidate(raw_input, selected)
    _set_cached_resolution(cache_key, payload)
    return payload


def search_birth_places(query: str, limit: int = 5) -> list[dict[str, Any]]:
    raw_input = (query or "").strip()
    if len(raw_input) < 2:
        return []

    cache_key = f"{_normalize_place_key(raw_input)}::{int(limit)}"
    cached = _get_cached_search(cache_key)
    if cached is not None:
        return cached[:limit]

    candidates = _PROVIDER.search(raw_input)
    if not candidates:
        return []

    scored = sorted(
        [(_candidate_score(raw_input, candidate), candidate) for candidate in candidates],
        key=lambda item: item[0],
        reverse=True,
    )
    results = []
    seen = set()
    for score, candidate in scored:
        if score < 1.5:
            continue
        serialized = _serialize_candidate(raw_input, candidate)
        dedupe_key = serialized["normalized_place"]
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        results.append(serialized)
        if len(results) >= limit:
            break

    _set_cached_search(cache_key, results)
    return results
