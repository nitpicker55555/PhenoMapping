#!/usr/bin/env python3
"""
Geocoder module for converting location names to coordinates
Uses a combination of hardcoded coordinates for known Bavarian locations
and fallback to approximate coordinates based on region
"""

# Known coordinates for Bavarian locations from 1856 data
LOCATION_COORDINATES = {
    # Oberpfalz (Upper Palatinate) region
    "Sulzbach": {"lat": 49.5047, "lon": 11.7456},
    "Kastl": {"lat": 49.3697, "lon": 11.6825},
    "Taubenbach": {"lat": 49.5369, "lon": 11.9647},
    "Wernberg": {"lat": 49.5403, "lon": 12.1547},
    "Freudenberg": {"lat": 49.5478, "lon": 11.8519},
    "Bodenwöhr": {"lat": 49.2750, "lon": 12.3078},
    "Hirschwald": {"lat": 49.3494, "lon": 11.7122},
    "Unterzell": {"lat": 49.4333, "lon": 11.9000},  # Approximate
    "Vilseck": {"lat": 49.6106, "lon": 11.8044},
    "Allersberg": {"lat": 49.2514, "lon": 11.2356},
    "Berg": {"lat": 49.3125, "lon": 11.3731},  # Berg bei Neumarkt
    "Richtheim": {"lat": 49.5500, "lon": 12.0833},  # Approximate (near Weiden)
    "Seligengarten": {"lat": 49.4500, "lon": 11.4333},  # Approximate
    "Hilpoltstein": {"lat": 49.1903, "lon": 11.1917},
    "Freihöls": {"lat": 49.3800, "lon": 11.7500},  # Approximate (near Kastl)
    
    # Special entries
    "Bemerkungen": {"lat": 49.4500, "lon": 11.8500},  # Center of region for "Remarks"
    "Unknown": {"lat": 49.4000, "lon": 11.7000},  # General Upper Palatinate center
    
    # Additional locations that might be referenced
    "Bamgersdorf": {"lat": 49.2833, "lon": 11.2667},  # Near Allersberg
    "Meischdorf": {"lat": 49.5500, "lon": 11.8333},  # Near Freudenberg
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