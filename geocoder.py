#!/usr/bin/env python3
"""
Geocoder module for converting location names to coordinates

This module provides coordinate lookup for pheno_new database locations.
The pheno_new database stores historical phenological observations from 1856,
but does NOT have coordinates in the database - all lat/lon fields are NULL.

Coordinates are provided here through:
1. Shapefile data: Extracted from 'Forstämter 5.shp' (Bavarian forest districts)
2. Manual research: For locations not found in shapefile

Data source: /Users/puzhen/Downloads/Forstämter 5.shp
Coordinate system: WGS84 (EPSG:4326)
"""

# =============================================================================
# PHENO_NEW DATABASE LOCATION COORDINATES
# =============================================================================
# These coordinates are used to display pheno_new historical observation data
# on maps in the distribution page (/distribution).
#
# The pheno_new database has these locations with observation counts:
#   - Unknown: 813 observations (excluded from mapping)
#   - Bemerkungen: 375 observations (general remarks, approximate location)
#   - Richtheim: 168 observations
#   - Wernberg: 124 observations
#   - Freudenberg: 105 observations
#   - Taubenbach: 82 observations
#   - Freihöls: 71 observations
#   - Sulzbach: 63 observations
#   - Hilpoltstein: 36 observations
#   - Kastl: 25 observations
#   - Berg: 22 observations
#   - Allersberg: 15 observations
# =============================================================================

LOCATION_COORDINATES = {
    # =========================================================================
    # COORDINATES FROM SHAPEFILE (Forstämter 5.shp)
    # These are centroids of forest district polygons, converted to WGS84
    # =========================================================================

    # Allersberg - Found in shapefile NAM column
    # Forstamt: Hilpoltstein (Oberpfalz)
    "Allersberg": {"lat": 49.1645, "lon": 11.1660},

    # Taubenbach - Found in shapefile NAM column
    # Forstamt: Amberg
    "Taubenbach": {"lat": 49.4454, "lon": 11.8214},

    # Wernberg - Found in shapefile Forstamt column
    # This is a Forstamt (forest district) itself
    "Wernberg": {"lat": 49.4939, "lon": 12.1602},

    # Hilpoltstein - Found in shapefile Forstamt column
    # Two districts exist: Oberpfalz and Mittelfranken, using Oberpfalz
    "Hilpoltstein": {"lat": 49.1645, "lon": 11.1660},

    # =========================================================================
    # APPROXIMATE MAPPING TO NEAREST FORSTAMT (相似映射)
    # These locations are NOT directly found in shapefile.
    # Coordinates are mapped to the centroid of the nearest/most similar
    # Forstamt polygon from the shapefile.
    # =========================================================================

    # Richtheim - Village in Landkreis Amberg-Sulzbach
    # 相似映射 → Forstamt Amberg (置信度: 中)
    # Shapefile 中无精确匹配，使用 Amberg 区域中心
    "Richtheim": {"lat": 49.4454, "lon": 11.8214},

    # Freudenberg - Municipality in Landkreis Amberg-Sulzbach
    # 相似映射 → Forstamt Amberg (置信度: 高)
    # 位于 Amberg 东北12km，Naabgebirge 山区
    "Freudenberg": {"lat": 49.4454, "lon": 11.8214},

    # Freihöls - Village in Landkreis Schwandorf
    # 相似映射 → Forstamt Burglengenfeld (置信度: 中)
    # 有两个同名地点：Schwandorf 市区和 Fensterbach 镇
    "Freihöls": {"lat": 49.1637, "lon": 11.9993},

    # Sulzbach - Part of Sulzbach-Rosenberg in Landkreis Amberg-Sulzbach
    # 相似映射 → Forstamt Neumarkt (置信度: 中)
    # Neumarkt 区域包含 Sulzbürg，与 Sulzbach 地区接近
    "Sulzbach": {"lat": 49.3023, "lon": 11.4962},

    # Kastl - Markt Kastl in Lauterachtal, Landkreis Amberg-Sulzbach
    # 相似映射 → Forstamt Amberg (置信度: 高)
    # 历史上有独立的 Forstamt Kastl，现映射到 Amberg 区域
    "Kastl": {"lat": 49.4454, "lon": 11.8214},

    # Berg - Berg bei Neumarkt in der Oberpfalz
    # 相似映射 → Forstamt Neumarkt (置信度: 中)
    "Berg": {"lat": 49.3023, "lon": 11.4962},

    # =========================================================================
    # ADDITIONAL LOCATIONS (may be referenced in historical records)
    # =========================================================================
    "Vilseck": {"lat": 49.6220, "lon": 11.7055},  # From shapefile Forstamt
    "Bodenwöhr": {"lat": 49.2750, "lon": 12.3078},
    "Hirschwald": {"lat": 49.3494, "lon": 11.7122},
    "Unterzell": {"lat": 49.4333, "lon": 11.9000},
    "Seligengarten": {"lat": 49.4500, "lon": 11.4333},
    "Bamgersdorf": {"lat": 49.2833, "lon": 11.2667},
    "Meischdorf": {"lat": 49.5500, "lon": 11.8333},

    # =========================================================================
    # SPECIAL ENTRIES
    # =========================================================================
    # Bemerkungen (Remarks) - Not a real location, use region center
    "Bemerkungen": {"lat": 49.4500, "lon": 11.8500},

    # Unknown - General Oberpfalz (Upper Palatinate) center
    "Unknown": {"lat": 49.4000, "lon": 11.7000},
}

def geocode_location(location_name):
    """
    Convert a location name to coordinates
    
    Args:
        location_name: Name of the location
        
    Returns:
        dict: {"name": location_name, "latitude": lat, "longitude": lon} or None if not found
    """
    if not location_name:
        return None
    
    # Clean up location name
    location_clean = location_name.strip()
    
    # Direct lookup
    if location_clean in LOCATION_COORDINATES:
        coords = LOCATION_COORDINATES[location_clean]
        return {
            "name": location_clean,
            "latitude": coords["lat"],
            "longitude": coords["lon"]
        }
    
    # Try partial matches for compound names
    for known_loc, coords in LOCATION_COORDINATES.items():
        if known_loc in location_clean or location_clean in known_loc:
            return {
                "name": location_clean,
                "latitude": coords["lat"],
                "longitude": coords["lon"]
            }
    
    # If not found, return None (we won't show unknown locations on map)
    return None

def geocode_locations(location_list):
    """
    Geocode a list of locations
    
    Args:
        location_list: List of location names
        
    Returns:
        list: List of geocoded locations with coordinates
    """
    geocoded = []
    for location in location_list:
        result = geocode_location(location)
        if result:
            geocoded.append(result)
    
    return geocoded

# For testing
if __name__ == "__main__":
    test_locations = ["Sulzbach", "Kastl", "Unknown Place", "Bemerkungen", "Freihöls"]
    for loc in test_locations:
        result = geocode_location(loc)
        if result:
            print(f"{loc}: {result['latitude']:.4f}, {result['longitude']:.4f}")
        else:
            print(f"{loc}: Not found")