"""Ayanamsa helpers and Swiss Ephemeris integration."""

from __future__ import annotations

import logging

import swisseph as swe

from config.astro_config import ASTRO_DEBUG

logger = logging.getLogger(__name__)


def _julian_day(datetime_utc):
    return swe.julday(
        datetime_utc.year,
        datetime_utc.month,
        datetime_utc.day,
        datetime_utc.hour + datetime_utc.minute / 60.0 + datetime_utc.second / 3600.0,
    )


def get_lahiri_offset(context):
    return float(swe.get_ayanamsa_ut(_julian_day(context.datetime_utc)))


def get_true_lahiri_offset(context):
    raise NotImplementedError(
        "Ayanamsa 'true_lahiri' is configured but not implemented with the current ephemeris integration."
    )


def get_ayanamsa_trace(context):
    requested = getattr(context, "ayanamsa", None)
    if requested == "lahiri":
        return {
            "ayanamsa_requested": requested,
            "ayanamsa_applied": "lahiri",
            "ayanamsa_supported": True,
            "sidereal_mode": "SIDM_LAHIRI",
        }
    if requested == "true_lahiri":
        return {
            "ayanamsa_requested": requested,
            "ayanamsa_applied": None,
            "ayanamsa_supported": False,
            "sidereal_mode": None,
            "warning": "true_lahiri_not_implemented",
        }
    return {
        "ayanamsa_requested": requested,
        "ayanamsa_applied": None,
        "ayanamsa_supported": False,
        "sidereal_mode": None,
        "warning": "unsupported_ayanamsa",
    }


def _unsupported_ayanamsa_error(context):
    trace = get_ayanamsa_trace(context)
    message = (
        f"Unsupported ayanamsa configuration: requested={trace['ayanamsa_requested']} "
        f"applied={trace['ayanamsa_applied']} supported={trace['ayanamsa_supported']}"
    )
    logger.warning(
        "Ayanamsa configuration unsupported ayanamsa_requested=%s ayanamsa_applied=%s ayanamsa_supported=%s",
        trace["ayanamsa_requested"],
        trace["ayanamsa_applied"],
        trace["ayanamsa_supported"],
    )
    raise NotImplementedError(message)


def get_ayanamsa_offset(context):
    trace = get_ayanamsa_trace(context)
    if not trace["ayanamsa_supported"]:
        _unsupported_ayanamsa_error(context)
    return get_lahiri_offset(context)


def get_sidereal_mode(context):
    trace = get_ayanamsa_trace(context)
    if not trace["ayanamsa_supported"]:
        _unsupported_ayanamsa_error(context)
    return swe.SIDM_LAHIRI


def configure_sidereal_mode(context):
    trace = get_ayanamsa_trace(context)
    logger_method = logger.info if ASTRO_DEBUG else logger.debug
    logger_method(
        "Ayanamsa trace ayanamsa_requested=%s ayanamsa_applied=%s ayanamsa_supported=%s",
        trace["ayanamsa_requested"],
        trace["ayanamsa_applied"],
        trace["ayanamsa_supported"],
    )
    if not trace["ayanamsa_supported"]:
        _unsupported_ayanamsa_error(context)
    swe.set_sid_mode(get_sidereal_mode(context))
    return trace


def apply_ayanamsa(tropical_longitude, context):
    offset = get_ayanamsa_offset(context)
    return (float(tropical_longitude) - offset) % 360
