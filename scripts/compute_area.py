#!/usr/bin/env python3
"""
Compute area of a GeoJSON polygon in km² using equal-area projection.
Adds the computed area as a property to the GeoJSON features.
"""

import json
import sys
from pathlib import Path

try:
    from shapely.geometry import shape
    from shapely.ops import transform
    import pyproj
except ImportError as e:
    print(f"Error: Required Python packages not found: {e}", file=sys.stderr)
    print("Please install: pip install shapely pyproj", file=sys.stderr)
    sys.exit(1)


def compute_area_km2(geojson_path):
    """
    Compute the area of a GeoJSON polygon in km².

    Args:
        geojson_path: Path to the GeoJSON file

    Returns:
        Area in km²
    """
    # Read GeoJSON
    with open(geojson_path, 'r') as f:
        data = json.load(f)

    if not data.get('features') or len(data['features']) == 0:
        print("Error: No features found in GeoJSON", file=sys.stderr)
        return None

    # Get the first feature's geometry
    feature = data['features'][0]
    geom = shape(feature['geometry'])

    # Define projection transformers
    # WGS84 (EPSG:4326) to World Mollweide (ESRI:54009) equal-area projection
    wgs84 = pyproj.CRS('EPSG:4326')
    mollweide = pyproj.CRS('ESRI:54009')

    project = pyproj.Transformer.from_crs(wgs84, mollweide, always_xy=True).transform

    # Transform geometry to equal-area projection
    geom_projected = transform(project, geom)

    # Compute area in square meters, convert to km²
    area_m2 = geom_projected.area
    area_km2 = round(area_m2 / 1_000_000, 2)

    return area_km2


def add_area_to_geojson(geojson_path, output_path=None):
    """
    Add computed area to GeoJSON properties.

    Args:
        geojson_path: Path to input GeoJSON file
        output_path: Path to output GeoJSON file (defaults to overwriting input)
    """
    # Compute area
    area_km2 = compute_area_km2(geojson_path)

    if area_km2 is None:
        sys.exit(1)

    # Read GeoJSON
    with open(geojson_path, 'r') as f:
        data = json.load(f)

    # Add area to properties
    data['features'][0]['properties']['area'] = area_km2

    # Write output
    if output_path is None:
        output_path = geojson_path

    with open(output_path, 'w') as f:
        json.dump(data, f)

    print(f"{area_km2}")
    return area_km2


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: compute_area.py <geojson_file> [output_file]", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    add_area_to_geojson(input_file, output_file)
