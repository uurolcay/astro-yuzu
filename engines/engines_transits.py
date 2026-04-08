import swisseph as swe
from datetime import datetime
import pytz

from config.astro_config import ASTRO_CONFIG
from core.ayanamsa import configure_sidereal_mode
from core.calculation_context import CalculationContext
from core.nodes import get_nodes

def _build_context(lat, lon):
    now = datetime.now(pytz.UTC)
    return CalculationContext(
        datetime_local=now,
        datetime_utc=now,
        latitude=lat,
        longitude=lon,
        timezone="UTC",
        ayanamsa=ASTRO_CONFIG["ayanamsa"],
        node_mode=ASTRO_CONFIG["node_mode"],
        house_system=ASTRO_CONFIG["house_system"],
    )


def get_current_transits(context_or_lat, lon=None):
    context = context_or_lat if isinstance(context_or_lat, CalculationContext) else _build_context(context_or_lat, lon)
    if isinstance(context_or_lat, CalculationContext):
        now_utc = datetime.now(pytz.UTC)
        now = now_utc
        try:
            local_now = now_utc.astimezone(pytz.timezone(context.timezone))
        except Exception:
            local_now = now_utc
        context = CalculationContext(
            datetime_local=local_now,
            datetime_utc=now_utc,
            latitude=context.latitude,
            longitude=context.longitude,
            timezone=context.timezone,
            ayanamsa=context.ayanamsa,
            node_mode=context.node_mode,
            house_system=context.house_system,
        )
    else:
        now = context.datetime_utc
    jd_now = swe.julday(now.year, now.month, now.day, now.hour + now.minute/60.0)
    configure_sidereal_mode(context)
    
    p_ids = [0, 1, 4, 2, 5, 3, 6]
    p_names = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn"]
    
    transits = []
    for pid, name in zip(p_ids, p_names):
        calc_result = swe.calc_ut(jd_now, pid, swe.FLG_SIDEREAL)
        res = calc_result[0] if isinstance(calc_result, (tuple, list)) else calc_result
        lon_val = float(res[0]) if isinstance(res, (tuple, list)) else float(res)
        
        transits.append({
            "name": name, 
            "sign_idx": int(lon_val / 30), 
            "degree": round(lon_val % 30, 4)
        })
    nodes = get_nodes(context)
    transits.append({"name": "Rahu", "sign_idx": int(nodes["rahu"] / 30), "degree": round(nodes["rahu"] % 30, 4)})
    transits.append({"name": "Ketu", "sign_idx": int(nodes["ketu"] / 30), "degree": round(nodes["ketu"] % 30, 4)})
    return transits

def score_current_impact(natal_data, transits):
    scores = []
    lagna_sign = natal_data['ascendant']['sign_idx']
    
    for tp in transits:
        house_num = ((tp['sign_idx'] - lagna_sign + 12) % 12) + 1
        base_score = 30
        if house_num in [1, 10]: base_score = 60
        
        for np in natal_data['planets']:
            if tp['sign_idx'] == np['sign_idx']:
                orb = abs(tp['degree'] - np['degree'])
                if orb < 6.0:
                    impact = base_score + 40
                    scores.append({
                        "event": f"Transit {tp['name']} on Natal {np['name']}",
                        "score": min(impact, 100),
                        "house": house_num
                    })
    return sorted(scores, key=lambda x: x['score'], reverse=True)
