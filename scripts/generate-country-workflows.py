#!/usr/bin/env python3
"""
Generate country metadata file for boundary extraction workflows.

This script fetches the list of ISO 3166-1 alpha-2 country codes and generates
a JSON data file containing metadata for all countries.
"""

import csv
import json
import sys
from pathlib import Path
from urllib.request import urlopen


# Configuration
ISO_CSV_URL = "https://raw.githubusercontent.com/ipregistry/iso3166/refs/heads/main/countries.csv"
OUTPUT_FILE = Path(__file__).parent.parent / ".github/data/countries.json"


def sanitize_tag(name: str) -> str:
    """Convert country name to a suitable tag."""
    return name.lower().replace(' ', '-').replace('(', '').replace(')', '').replace(',', '')


def main():
    """Generate country metadata JSON file."""

    # Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    print(f"Fetching ISO country codes from {ISO_CSV_URL}...")

    # Fetch and parse CSV
    with urlopen(ISO_CSV_URL) as response:
        content = response.read().decode('utf-8')

    # Remove the '#' from the header line if present
    lines = content.splitlines()
    if lines and lines[0].startswith('#'):
        lines[0] = lines[0][1:]  # Remove leading '#'

    countries_list = list(csv.DictReader(lines))
    total_countries = len(countries_list)

    print(f"Found {total_countries} countries")
    print(f"Generating country metadata file...")

    countries_data = {}

    for country in countries_list:
        iso = country['country_code_alpha2'].upper()  # Ensure uppercase
        name_short = country['name_short']

        iso_lower = iso.lower()

        # Generate tag from country name
        tag_name = sanitize_tag(name_short)

        # Build country metadata
        countries_data[iso_lower] = {
            "code": iso_lower,
            "name": name_short,
            "osm_query": f'a["ISO3166-1:alpha2"="{iso}"]',
            "tag_name": tag_name
        }

    # Write JSON file
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(countries_data, f, indent=2, sort_keys=True)

    print(f"\n✅ Generation complete!")
    print(f"   Generated metadata for {len(countries_data)} countries")
    print(f"   Output: {OUTPUT_FILE}")

    # Show sample entries
    print(f"\n   Sample entries:")
    sample_codes = ['fr', 'de', 'us', 'jp', 'br']
    for code in sample_codes:
        if code in countries_data:
            print(f"   {code.upper()}: {countries_data[code]['name']}")


if __name__ == "__main__":
    main()
