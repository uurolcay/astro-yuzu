"""Structured context for deterministic astrology calculations."""

from dataclasses import dataclass


@dataclass(frozen=True)
class CalculationContext:
    datetime_local: object
    datetime_utc: object
    latitude: float
    longitude: float
    timezone: str
    ayanamsa: str
    node_mode: str
    house_system: str

