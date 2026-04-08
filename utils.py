import pytz
from datetime import datetime

from services.geocoding import BirthPlaceResolutionError, resolve_birth_place


def combine_birth_place_input(place_text, country_name=""):
    query_parts = [part.strip() for part in [place_text, country_name] if part and str(part).strip()]
    return ", ".join(query_parts)


def build_birth_context(birth_date_str, resolved_location):
    timezone_str = resolved_location["timezone"]
    local_tz = pytz.timezone(timezone_str)
    try:
        naive_dt = datetime.strptime(birth_date_str, "%Y-%m-%dT%H:%M")
    except ValueError:
        naive_dt = datetime.strptime(birth_date_str, "%Y-%m-%d")
        naive_dt = naive_dt.replace(hour=12, minute=0)

    local_dt = local_tz.localize(naive_dt, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.UTC)
    return {
        "local_datetime": local_dt,
        "utc_datetime": utc_dt,
        "timezone": timezone_str,
        "latitude": resolved_location["latitude"],
        "longitude": resolved_location["longitude"],
        "normalized_birth_place": resolved_location["normalized_place"],
        "raw_birth_place_input": resolved_location["raw_input"],
        "geocode_provider": resolved_location["provider"],
        "geocode_confidence": resolved_location.get("confidence"),
    }


def resolve_birth_location(place_text, country_name=""):
    combined_input = combine_birth_place_input(place_text, country_name)
    return resolve_birth_place(combined_input)


def get_utc_and_coords(birth_date_str, city_name, country_name=""):
    resolved_location = resolve_birth_location(city_name, country_name)
    birth_context = build_birth_context(birth_date_str, resolved_location)
    return birth_context["utc_datetime"], birth_context["latitude"], birth_context["longitude"]
