#!/usr/bin/env python3
"""
Generate individual GitHub Actions workflows for each country boundary extraction.

This script fetches the list of ISO 3166-1 alpha-2 country codes and generates
a separate workflow file for each country. The workflows are staggered across
the week to avoid overwhelming the runner.
"""

import csv
import os
import sys
from pathlib import Path
from urllib.request import urlopen


# Configuration
ISO_CSV_URL = "https://raw.githubusercontent.com/ipregistry/iso3166/refs/heads/main/countries.csv"
WORKFLOWS_DIR = Path(__file__).parent.parent / ".github/workflows/boundaries/countries"
TEMPLATE = """name: Country Boundary for {name_short} ({iso})

on:
  schedule:
    - cron: '{cron}'  # {schedule_description}
  workflow_dispatch:

jobs:
  boundary-country-{iso_lower}:
    uses: ./.github/workflows/boundaries/reusable-boundaries.yaml
    with:
      entity_code: {iso_lower}
      entity_name: {name_short}
      entity_type: country
      osm_query: 'a["ISO3166-1:alpha2"="{iso}"]'
      remote_path: /osm/boundaries/countries
      remote_version: '1'
      tags: |
        boundary
        country
        {tag_name}
        geojson
        openstreetmap
        public
    secrets:
      RCLONE_CONFIG_DATA: ${{{{ secrets.RCLONE_CONFIG_DATA }}}}
"""


def generate_cron_schedule(index: int, total: int) -> tuple[str, str]:
    """
    Generate a staggered cron schedule to distribute workflows across the month.

    Returns: (cron_expression, description)
    """
    # Distribute across 30 days of the month, with multiple runs per day if needed
    countries_per_day = (total + 29) // 30

    day_of_month = (index // countries_per_day) % 30 + 1  # 1-30
    time_slot = index % countries_per_day

    # Distribute time slots across the day (starting at 2:00 AM)
    # Use 5-minute intervals to avoid overwhelming the system
    start_hour = 2
    minutes = (time_slot * 5) % 60
    hour = start_hour + (time_slot * 5) // 60

    cron = f"{minutes} {hour} {day_of_month} * *"

    description = f"Monthly on day {day_of_month} at {hour:02d}:{minutes:02d} UTC"

    return cron, description


def sanitize_tag(name: str) -> str:
    """Convert country name to a suitable tag."""
    return name.lower().replace(' ', '-').replace('(', '').replace(')', '').replace(',', '')


def main():
    """Generate workflow files for all countries."""

    # Ensure output directory exists
    WORKFLOWS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching ISO country codes from {ISO_CSV_URL}...")

    # Fetch and parse CSV
    with urlopen(ISO_CSV_URL) as response:
        content = response.read().decode('utf-8')

    # Remove the '#' from the header line if present
    lines = content.splitlines()
    if lines and lines[0].startswith('#'):
        lines[0] = lines[0][1:]  # Remove leading '#'

    countries = list(csv.DictReader(lines))
    total_countries = len(countries)

    print(f"Found {total_countries} countries")
    print(f"Generating workflow files in {WORKFLOWS_DIR}...")

    generated_count = 0
    skipped_count = 0

    for index, country in enumerate(countries):
        iso = country['country_code_alpha2'].upper()  # Ensure uppercase
        name_short = country['name_short']
        name_long = country['name_long']

        iso_lower = iso.lower()
        output_file = WORKFLOWS_DIR / f"{iso}.yaml"  # Use uppercase for filename

        # Generate staggered cron schedule
        cron, schedule_desc = generate_cron_schedule(index, total_countries)

        # Generate tag from country name
        tag_name = sanitize_tag(name_short)

        # Generate workflow content
        workflow_content = TEMPLATE.format(
            iso=iso,
            iso_lower=iso_lower,
            name_short=name_short,
            name_long=name_long,
            tag_name=tag_name,
            cron=cron,
            schedule_description=schedule_desc
        )

        # Write workflow file
        try:
            with open(output_file, 'w') as f:
                f.write(workflow_content)
            generated_count += 1

            if (index + 1) % 50 == 0:
                print(f"  Generated {index + 1}/{total_countries} workflows...")
        except Exception as e:
            print(f"  ⚠️ Failed to generate {iso}: {e}", file=sys.stderr)
            skipped_count += 1

    print(f"\n✅ Generation complete!")
    print(f"   Generated: {generated_count} workflows")
    if skipped_count > 0:
        print(f"   Skipped: {skipped_count} workflows")
    print(f"   Location: {WORKFLOWS_DIR}")

    # Print distribution summary
    print(f"\n📅 Schedule Distribution:")
    countries_per_day = (total_countries + 29) // 30
    print(f"   Distributed across 30 days of the month")
    print(f"   ~{countries_per_day} countries per day")
    print(f"   Time slots: 5-minute intervals starting at 2:00 AM UTC")

    # Show a sample of the distribution
    print(f"\n   Sample schedule:")
    for day in [1, 5, 10, 15, 20, 25, 30]:
        count = min(countries_per_day, total_countries - ((day - 1) * countries_per_day))
        if count > 0:
            print(f"   Day {day:2d}: ~{count} countries")


if __name__ == "__main__":
    main()
