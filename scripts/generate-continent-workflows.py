#!/usr/bin/env python3
"""
Generate individual GitHub Actions workflows for each continent boundary extraction.

This script generates a workflow file for each continent with staggered schedules.
"""

import os
import sys
from pathlib import Path


# Configuration
WORKFLOWS_DIR = Path(__file__).parent.parent / ".github/workflows/boundaries/continents"

# Continent data: (code, name, osm_relation_id)
# OSM relation IDs for continents from OpenStreetMap
CONTINENTS = [
    ("af", "Africa", "192784"),
    ("an", "Antarctica", "2186646"),
    ("as", "Asia", "214656"),
    ("eu", "Europe", "52822"),
    ("na", "North America", "49428"),
    ("oc", "Oceania", "112224"),
    ("sa", "South America", "52805"),
]

TEMPLATE = """name: Continent Boundary for {name} ({code_upper})

on:
  schedule:
    - cron: '{cron}'  # {schedule_description}
  workflow_dispatch:

jobs:
  boundary-continent-{code}:
    uses: ./.github/workflows/boundaries/reusable-boundaries.yaml
    with:
      entity_code: {code}
      entity_name: {name}
      entity_type: continent
      osm_query: 'r{osm_id}'
      remote_path: /osm/boundaries/continents/{code}
      remote_version: '1'
      tags: |
        boundary
        continent
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
    # Distribute across 7 days (one per continent)
    day_of_month = index + 1  # Days 1-7
    base_hour = 3  # Start at 3:00 AM UTC
    minutes = (index * 10) % 60  # Stagger by 10 minutes
    hour = base_hour + (index * 10) // 60  # Handle hour overflow

    cron = f"{minutes} {hour} {day_of_month} * *"
    description = f"Monthly on day {day_of_month} at {hour:02d}:{minutes:02d} UTC"

    return cron, description


def sanitize_tag(name: str) -> str:
    """Convert continent name to a suitable tag."""
    return name.lower().replace(' ', '-')


def main():
    """Generate workflow files for all continents."""

    # Ensure output directory exists
    WORKFLOWS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Generating continent workflow files in {WORKFLOWS_DIR}...")

    total_continents = len(CONTINENTS)
    generated_count = 0

    for index, (code, name, osm_id) in enumerate(CONTINENTS):
        code_upper = code.upper()
        output_file = WORKFLOWS_DIR / f"{code_upper}.yaml"

        # Generate staggered cron schedule
        cron, schedule_desc = generate_cron_schedule(index, total_continents)

        # Generate tag from continent name
        tag_name = sanitize_tag(name)

        # Generate workflow content
        workflow_content = TEMPLATE.format(
            code=code,
            code_upper=code_upper,
            name=name,
            osm_id=osm_id,
            tag_name=tag_name,
            cron=cron,
            schedule_description=schedule_desc
        )

        # Write workflow file
        try:
            with open(output_file, 'w') as f:
                f.write(workflow_content)
            generated_count += 1
            print(f"  ✓ Generated {code_upper}: {name}")
        except Exception as e:
            print(f"  ⚠️ Failed to generate {code_upper}: {e}", file=sys.stderr)

    print(f"\n✅ Generation complete!")
    print(f"   Generated: {generated_count} workflows")
    print(f"   Location: {WORKFLOWS_DIR}")

    print(f"\n📅 Schedule Distribution:")
    print(f"   One continent per day (days 1-7 of each month)")
    print(f"   Time: 3:00 AM UTC, staggered by 10 minutes")

    print(f"\n   Schedule:")
    for index, (code, name, _) in enumerate(CONTINENTS):
        cron, schedule_desc = generate_cron_schedule(index, total_continents)
        base_hour = 3
        minutes = (index * 10) % 60
        hour = base_hour + (index * 10) // 60
        print(f"   Day {index + 1:2d}, {hour}:{minutes:02d} AM UTC: {name} ({code.upper()})")


if __name__ == "__main__":
    main()
