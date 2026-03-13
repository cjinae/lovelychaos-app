from __future__ import annotations

import csv
import io
import json

import httpx

from app.services.school_directory import DATA_PATH, GTA_CITY_NAMES, GTA_TIMEZONE


SOURCE_URL = (
    "https://data.ontario.ca/dataset/"
    "fb3a7c18-90af-453e-bc0a-a76ecc471862/resource/"
    "f3a8c2a3-09d9-4715-9044-d8a0189f572c/download/"
    "public_school_contact_list_february2026_en.txt"
)


def main() -> int:
    response = httpx.get(SOURCE_URL, timeout=60)
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.text), delimiter="|")
    schools: list[dict] = []
    for row in reader:
        city = (row.get("City") or "").strip()
        if city not in GTA_CITY_NAMES:
            continue
        school_name = (row.get("School Name") or "").strip()
        if not school_name:
            continue
        schools.append(
            {
                "school_name": school_name,
                "board_name": (row.get("Board Name") or "").strip(),
                "city": city,
                "province": (row.get("Province") or "").strip(),
                "postal_code": (row.get("Postal Code") or "").strip(),
                "street": " ".join(
                    part for part in [(row.get("Suite") or "").strip(), (row.get("Street") or "").strip()] if part
                ),
                "school_level": (row.get("School Level") or "").strip(),
                "school_language": (row.get("School Language") or "").strip(),
                "school_type": (row.get("School Type") or "").strip(),
                "timezone": GTA_TIMEZONE,
                "source": "ontario_public_school_contact_information_february_2026",
            }
        )

    schools.sort(key=lambda item: (item["school_name"], item["city"], item["board_name"]))
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_PATH.write_text(json.dumps(schools, indent=2))
    print(f"Wrote {len(schools)} schools to {DATA_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
