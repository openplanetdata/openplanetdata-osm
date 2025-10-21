#!/usr/bin/env python3
"""
Generate continent metadata file for boundary extraction workflows.

This script generates a JSON data file containing metadata for all continents.
"""

import json
import sys
from pathlib import Path


# Configuration
OUTPUT_FILE = Path(__file__).parent.parent / ".github/data/continents.json"

# Continent data: (code, name)
CONTINENTS = [
    ("af", "Africa"),
    ("an", "Antarctica"),
    ("as", "Asia"),
    ("eu", "Europe"),
    ("na", "North America"),
    ("oc", "Oceania"),
    ("sa", "South America"),
]


def sanitize_tag(name: str) -> str:
    """Convert continent name to a suitable tag."""
    return name.lower().replace(' ', '-')


def main():
    """Generate continent metadata JSON file."""

    # Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating continent metadata file...")

    continents_data = {}

    for code, name in CONTINENTS:
        # Generate tag from continent name
        tag_name = sanitize_tag(name)

        # Build continent metadata
        continents_data[code] = {
            "code": code,
            "name": name,
            "osm_query": f'a[place=continent]["name:en"="{name}"]',
            "tag_name": tag_name
        }

    # Write JSON file
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(continents_data, f, indent=2, sort_keys=True)

    print(f"\n✅ Generation complete!")
    print(f"   Generated metadata for {len(continents_data)} continents")
    print(f"   Output: {OUTPUT_FILE}")

    # Show all entries
    print(f"\n   Continents:")
    for code in sorted(continents_data.keys()):
        print(f"   {code.upper()}: {continents_data[code]['name']}")


if __name__ == "__main__":
    main()
