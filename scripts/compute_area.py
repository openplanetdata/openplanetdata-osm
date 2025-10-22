#!/usr/bin/env python3
"""
Compute area of a GeoJSON polygon in km² using geodesic calculation.
Returns the computed area value.
"""

import json
import sys

try:
    from pyproj import Geod
except ImportError as e:
    print(f"Error: Required Python package not found: {e}", file=sys.stderr)
    print("Please install: pip install pyproj", file=sys.stderr)
    sys.exit(1)


def compute_polygon_area(coords, geod):
    """
    Compute area of a single polygon ring.

    Args:
        coords: List of [lon, lat] coordinate pairs
        geod: pyproj Geod object

    Returns:
        Area in square meters (negative for holes)
    """
    lons = [coord[0] for coord in coords]
    lats = [coord[1] for coord in coords]
    area, _ = geod.polygon_area_perimeter(lons, lats)
    return area


def compute_area_km2(geojson_path):
    """
    Compute the geodesic area of a GeoJSON polygon in km².
    Uses WGS84 ellipsoid for accurate area calculation.

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
    geometry = feature['geometry']
    geom_type = geometry['type']

    # Use pyproj Geod for geodesic area calculation on WGS84 ellipsoid
    geod = Geod(ellps='WGS84')

    total_area = 0.0

    if geom_type == 'Polygon':
        # Polygon has [exterior, hole1, hole2, ...]
        coords = geometry['coordinates']
        # Exterior ring
        exterior_area = compute_polygon_area(coords[0], geod)
        total_area = abs(exterior_area)
        # Subtract holes
        for hole in coords[1:]:
            hole_area = compute_polygon_area(hole, geod)
            total_area -= abs(hole_area)

    elif geom_type == 'MultiPolygon':
        # MultiPolygon is a list of Polygons
        for polygon in geometry['coordinates']:
            # Each polygon has [exterior, hole1, hole2, ...]
            exterior_area = compute_polygon_area(polygon[0], geod)
            poly_area = abs(exterior_area)
            # Subtract holes
            for hole in polygon[1:]:
                hole_area = compute_polygon_area(hole, geod)
                poly_area -= abs(hole_area)
            total_area += poly_area

    else:
        print(f"Error: Unsupported geometry type: {geom_type}", file=sys.stderr)
        return None

    # Convert to km²
    area_km2 = round(total_area / 1_000_000, 2)

    return area_km2


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: compute_area.py <geojson_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    area_km2 = compute_area_km2(input_file)

    if area_km2 is None:
        sys.exit(1)

    print(f"{area_km2}")
