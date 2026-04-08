import swisseph as swe

from config.astro_config import ASTRO_CONFIG, ASTRO_ENGINE_VERSION
from core.ayanamsa import configure_sidereal_mode, get_ayanamsa_trace
from core.calculation_context import CalculationContext
from core.nodes import get_nodes

NAKSHATRA_NAMES = [
    "Ashwini", "Bharani", "Krittika", "Rohini", "Mrigashirsha", "Ardra",
    "Punarvasu", "Pushya", "Ashlesha", "Magha", "Purva Phalguni", "Uttara Phalguni",
    "Hasta", "Chitra", "Swati", "Vishakha", "Anuradha", "Jyeshtha",
    "Mula", "Purva Ashadha", "Uttara Ashadha", "Shravana", "Dhanishta",
    "Shatabhisha", "Purva Bhadrapada", "Uttara Bhadrapada", "Revati",
]


def get_nakshatra_name(longitude):
    nakshatra_span = 360 / 27
    nakshatra_index = int(longitude / nakshatra_span) % 27
    return NAKSHATRA_NAMES[nakshatra_index]


def _build_context(datetime_utc, lat, lon):
    return CalculationContext(
        datetime_local=datetime_utc,
        datetime_utc=datetime_utc,
        latitude=lat,
        longitude=lon,
        timezone="UTC",
        ayanamsa=ASTRO_CONFIG["ayanamsa"],
        node_mode=ASTRO_CONFIG["node_mode"],
        house_system=ASTRO_CONFIG["house_system"],
    )


def calculate_natal_data(context_or_utc_dt, lat=None, lon=None):
    context = context_or_utc_dt if isinstance(context_or_utc_dt, CalculationContext) else _build_context(context_or_utc_dt, lat, lon)
    if context.house_system != "whole_sign":
        raise ValueError("Unsupported house system in production mode")

    decimal_hour = context.datetime_utc.hour + context.datetime_utc.minute / 60.0 + context.datetime_utc.second / 3600.0
    jd = swe.julday(context.datetime_utc.year, context.datetime_utc.month, context.datetime_utc.day, decimal_hour)

    configure_sidereal_mode(context)

    p_ids = [0, 1, 4, 2, 5, 3, 6]  # Sun, Moon, Mars, Mercury, Jupiter, Venus, Saturn
    p_names = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn"]

    planets_data = []
    for pid, name in zip(p_ids, p_names):
        calc_result = swe.calc_ut(jd, pid, swe.FLG_SIDEREAL | swe.FLG_SPEED)
        res = calc_result[0] if isinstance(calc_result, (tuple, list)) else calc_result

        lon_abs = float(res[0]) if isinstance(res, (tuple, list)) else float(res)
        speed = float(res[3]) if isinstance(res, (tuple, list)) and len(res) > 3 else 0.0

        planets_data.append(
            {
                "name": name,
                "abs_longitude": round(lon_abs, 4),
                "sign_idx": int(lon_abs / 30),
                "degree": round(lon_abs % 30, 4),
                "nakshatra": get_nakshatra_name(lon_abs),
                "is_retrograde": speed < 0,
            }
        )

    node_data = get_nodes(context)
    rahu_lon = node_data["rahu"]
    planets_data.append(
        {
            "name": "Rahu",
            "abs_longitude": round(rahu_lon, 4),
            "sign_idx": int(rahu_lon / 30),
            "degree": round(rahu_lon % 30, 4),
            "nakshatra": get_nakshatra_name(rahu_lon),
            "is_retrograde": node_data["speed"] < 0,
        }
    )
    ketu_lon = node_data["ketu"]
    planets_data.append(
        {
            "name": "Ketu",
            "abs_longitude": round(ketu_lon, 4),
            "sign_idx": int(ketu_lon / 30),
            "degree": round(ketu_lon % 30, 4),
            "nakshatra": get_nakshatra_name(ketu_lon),
            "is_retrograde": True,
        }
    )

    houses_data = swe.houses_ex(jd, context.latitude, context.longitude, b"W", swe.FLG_SIDEREAL)
    ascmc = houses_data[1]
    lagna_lon = float(ascmc[0])
    lagna_sign = int(lagna_lon / 30)

    for planet in planets_data:
        planet["house"] = ((planet["sign_idx"] - lagna_sign + 12) % 12) + 1

    main_7 = [planet for planet in planets_data if planet["name"] not in ["Rahu", "Ketu"]]
    sorted_p = sorted(main_7, key=lambda x: x["degree"], reverse=True)

    return {
        "planets": planets_data,
        "ascendant": {
            "name": "Lagna",
            "sign_idx": lagna_sign,
            "degree": round(lagna_lon % 30, 4),
            "abs_longitude": round(lagna_lon, 4),
        },
        "karakas": {"atmakaraka": sorted_p[0]["name"], "amatyakaraka": sorted_p[1]["name"]},
        "calculation_config": {
            "engine_version": ASTRO_ENGINE_VERSION,
            "ayanamsa": context.ayanamsa,
            "node_mode": context.node_mode,
            "house_system": context.house_system,
            "zodiac": ASTRO_CONFIG["zodiac"],
            **get_ayanamsa_trace(context),
        },
    }
