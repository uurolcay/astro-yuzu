"""Helpers for persisting calculation standard snapshots."""

from __future__ import annotations

from config.astro_config import ASTRO_CONFIG, ASTRO_ENGINE_VERSION
from core.ayanamsa import get_ayanamsa_trace


def build_calculation_metadata_snapshot(*, calculation_context, birth_context=None):
    birth_context = birth_context or {}
    trace = get_ayanamsa_trace(calculation_context)
    return {
        "engine_version": ASTRO_ENGINE_VERSION,
        "zodiac": ASTRO_CONFIG["zodiac"],
        "ayanamsa": calculation_context.ayanamsa,
        "node_mode": calculation_context.node_mode,
        "house_system": calculation_context.house_system,
        "timezone": calculation_context.timezone,
        "latitude": calculation_context.latitude,
        "longitude": calculation_context.longitude,
        "normalized_birth_place": birth_context.get("normalized_birth_place"),
        "geocode_provider": birth_context.get("geocode_provider"),
        "location_source": birth_context.get("location_source"),
        "geocode_cache_hit": birth_context.get("geocode_cache_hit"),
        **trace,
    }
