"""Node mode standardization helpers."""

from __future__ import annotations

import swisseph as swe

from core.ayanamsa import configure_sidereal_mode


def _node_longitude(context, node_id):
    configure_sidereal_mode(context)
    jd = swe.julday(
        context.datetime_utc.year,
        context.datetime_utc.month,
        context.datetime_utc.day,
        context.datetime_utc.hour + context.datetime_utc.minute / 60.0 + context.datetime_utc.second / 3600.0,
    )
    calc_result = swe.calc_ut(jd, node_id, swe.FLG_SIDEREAL | swe.FLG_SPEED)
    result = calc_result[0] if isinstance(calc_result, (tuple, list)) else calc_result
    return float(result[0]), float(result[3]) if isinstance(result, (tuple, list)) and len(result) > 3 else 0.0


def compute_mean_node(context):
    return _node_longitude(context, swe.MEAN_NODE)


def compute_true_node(context):
    return _node_longitude(context, swe.TRUE_NODE)


def get_nodes(context):
    if context.node_mode == "mean":
        rahu_lon, speed = compute_mean_node(context)
    elif context.node_mode == "true":
        rahu_lon, speed = compute_true_node(context)
    else:
        raise ValueError(f"Unsupported node mode: {context.node_mode}")
    ketu_lon = (rahu_lon + 180.0) % 360.0
    return {"rahu": rahu_lon, "ketu": ketu_lon, "speed": speed}

