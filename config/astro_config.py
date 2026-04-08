"""Centralized astrology calculation configuration."""

ASTRO_ENGINE_VERSION = "2026.04-parity-2"

ASTRO_CONFIG = {
    "zodiac": "sidereal",
    "ayanamsa": "lahiri",
    "node_mode": "true",
    "house_system": "whole_sign",
    "ephemeris": "swisseph",
    "default_timezone_strategy": "geo",
    "astro_debug": True,
    "strict_astro_mode": True,
}

ASTRO_DEBUG = ASTRO_CONFIG["astro_debug"]
STRICT_ASTRO_MODE = ASTRO_CONFIG["strict_astro_mode"]
