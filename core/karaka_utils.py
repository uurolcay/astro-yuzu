from __future__ import annotations

from copy import deepcopy


CLASSICAL_KARAKA_PLANETS = ("Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn")
RAHU_EIGHT_KARAKA_PLANETS = CLASSICAL_KARAKA_PLANETS + ("Rahu",)


def select_atmakaraka(planets, mode="7"):
    selected = _sorted_karaka_candidates(planets, mode=mode)
    return selected[0] if selected else {}


def select_chara_karakas(planets, mode="7"):
    return _sorted_karaka_candidates(planets, mode=mode)


def _sorted_karaka_candidates(planets, *, mode="7"):
    normalized_mode = "8" if str(mode or "7").strip() == "8" else "7"
    allowed = set(RAHU_EIGHT_KARAKA_PLANETS if normalized_mode == "8" else CLASSICAL_KARAKA_PLANETS)
    candidates = []
    for row in list(planets or []):
        name = str((row or {}).get("name") or "").strip()
        if not name or name == "Ketu" or name not in allowed:
            continue
        degree = karaka_degree_value(row, mode=normalized_mode)
        if degree is None:
            continue
        candidates.append(
            {
                "planet": name,
                "degree_value": degree,
                "raw_degree": (row or {}).get("degree"),
                "planet_row": deepcopy(row),
                "rahu_adjusted": name == "Rahu",
                "karaka_mode": normalized_mode,
                "calculation_basis": "degree_within_sign",
            }
        )
    return sorted(candidates, key=lambda item: item["degree_value"], reverse=True)


def karaka_degree_value(row, mode="7"):
    name = str((row or {}).get("name") or "").strip()
    value = (row or {}).get("degree")
    if value in (None, ""):
        return None
    try:
        degree = float(value)
    except (TypeError, ValueError):
        return None
    normalized_mode = "8" if str(mode or "7").strip() == "8" else "7"
    if name == "Ketu":
        return None
    if name == "Rahu":
        if normalized_mode != "8":
            return None
        return round(30.0 - degree, 6)
    return degree
