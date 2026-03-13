from __future__ import annotations

from dataclasses import asdict, dataclass
from functools import lru_cache
import json
from pathlib import Path
import re
from typing import Optional

import httpx
from timezonefinder import TimezoneFinder


GTA_TIMEZONE = "America/Toronto"
GTA_CITY_NAMES = {
    "Ajax",
    "Aurora",
    "Bowmanville",
    "Brampton",
    "Burlington",
    "Caledon",
    "East Gwillimbury",
    "East York",
    "Etobicoke",
    "Georgetown",
    "Georgina",
    "Halton Hills",
    "Keswick",
    "King City",
    "Markham",
    "Milton",
    "Mississauga",
    "Newmarket",
    "Nobleton",
    "North York",
    "Oakville",
    "Oshawa",
    "Pickering",
    "Richmond Hill",
    "Scarborough",
    "Toronto",
    "Vaughan",
    "Whitby",
    "Whitchurch-Stouffville",
    "York",
}
DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "gta_public_schools_2026_02.json"
_TIMEZONE_FINDER = TimezoneFinder(in_memory=True)


@dataclass(frozen=True)
class SchoolEntry:
    school_name: str
    board_name: str
    city: str
    province: str
    postal_code: str
    street: str
    school_level: str
    school_language: str
    school_type: str
    timezone: str = GTA_TIMEZONE
    source: str = "ontario_public_school_contact_information_february_2026"


@dataclass(frozen=True)
class SchoolResolution:
    school_name: str
    timezone: str
    city: str = ""
    province: str = ""
    board_name: str = ""
    postal_code: str = ""
    source: str = ""
    matched_from_directory: bool = False

    def as_dict(self) -> dict:
        return asdict(self)


@lru_cache(maxsize=1)
def load_gta_school_directory() -> list[SchoolEntry]:
    rows = json.loads(DATA_PATH.read_text())
    return [SchoolEntry(**row) for row in rows]


def search_gta_schools(query: str, limit: int = 8) -> list[dict]:
    normalized_query = _normalize_text(query)
    if len(normalized_query) < 2:
        return []

    tokens = normalized_query.split()
    scored: list[tuple[tuple[int, int, str], SchoolEntry]] = []
    for entry in load_gta_school_directory():
        name_norm = _normalize_text(entry.school_name)
        city_norm = _normalize_text(entry.city)
        board_norm = _normalize_text(entry.board_name)
        if normalized_query not in name_norm and normalized_query not in city_norm and normalized_query not in board_norm:
            if not all(token in f"{name_norm} {city_norm} {board_norm}" for token in tokens):
                continue
        score = 0
        if name_norm.startswith(normalized_query):
            score += 120
        if normalized_query in name_norm:
            score += 60
        score += sum(18 for token in tokens if token in name_norm)
        score += sum(8 for token in tokens if token in city_norm or token in board_norm)
        score += max(0, 20 - abs(len(entry.school_name) - len(query.strip())))
        scored.append(((-score, len(entry.school_name), entry.school_name), entry))

    scored.sort(key=lambda item: item[0])
    results: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for _, entry in scored:
        key = (entry.school_name, entry.city)
        if key in seen:
            continue
        seen.add(key)
        results.append(asdict(entry))
        if len(results) >= limit:
            break
    return results


def resolve_school_timezone(school_name: str, client: Optional[httpx.Client] = None) -> Optional[SchoolResolution]:
    normalized_query = _normalize_text(school_name)
    if not normalized_query:
        return None

    local = _resolve_from_directory(normalized_query)
    if local is not None:
        return local

    return _resolve_from_geocoder(school_name, client=client)


def _resolve_from_directory(normalized_query: str) -> Optional[SchoolResolution]:
    best_entry: Optional[SchoolEntry] = None
    best_score = -1
    for entry in load_gta_school_directory():
        name_norm = _normalize_text(entry.school_name)
        if normalized_query == name_norm:
            best_entry = entry
            best_score = 10_000
            break
        if normalized_query in name_norm or name_norm in normalized_query:
            score = 500 - abs(len(name_norm) - len(normalized_query))
            if score > best_score:
                best_score = score
                best_entry = entry
    if best_entry is None:
        return None
    return SchoolResolution(
        school_name=best_entry.school_name,
        timezone=best_entry.timezone,
        city=best_entry.city,
        province=best_entry.province,
        board_name=best_entry.board_name,
        postal_code=best_entry.postal_code,
        source=best_entry.source,
        matched_from_directory=True,
    )


def _resolve_from_geocoder(school_name: str, client: Optional[httpx.Client] = None) -> Optional[SchoolResolution]:
    own_client = client is None
    active_client = client or httpx.Client(timeout=20)
    try:
        response = active_client.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": f"{school_name} school",
                "format": "jsonv2",
                "addressdetails": 1,
                "limit": 5,
                "countrycodes": "ca",
            },
            headers={"User-Agent": "LovelyChaos/1.0 school-resolution"},
        )
        response.raise_for_status()
        results = response.json()
    except Exception:
        return None
    finally:
        if own_client:
            active_client.close()

    for candidate in results:
        address = candidate.get("address") or {}
        timezone_name = _timezone_for_coordinates(candidate.get("lat"), candidate.get("lon"))
        if not timezone_name:
            continue
        return SchoolResolution(
            school_name=_best_geocoder_name(candidate, school_name),
            timezone=timezone_name,
            city=_best_city(address),
            province=address.get("state", ""),
            postal_code=address.get("postcode", ""),
            source="openstreetmap_nominatim",
            matched_from_directory=False,
        )
    return None


def _best_geocoder_name(candidate: dict, fallback_name: str) -> str:
    address = candidate.get("address") or {}
    return (
        address.get("school")
        or address.get("building")
        or address.get("amenity")
        or candidate.get("name")
        or fallback_name
    )


def _best_city(address: dict) -> str:
    return (
        address.get("city")
        or address.get("town")
        or address.get("borough")
        or address.get("suburb")
        or address.get("municipality")
        or ""
    )


def _timezone_for_coordinates(lat_value: object, lon_value: object) -> Optional[str]:
    try:
        latitude = float(lat_value)
        longitude = float(lon_value)
    except (TypeError, ValueError):
        return None
    return _TIMEZONE_FINDER.timezone_at(lat=latitude, lng=longitude)


def _normalize_text(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", (value or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()
