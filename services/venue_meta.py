# services/venue_meta.py
"""MLB venue metadata: coordinates, dome status, altitude, orientation."""

# venue_id → {lat, lon, dome, alt_ft, name, bearing_to_cf}
# bearing_to_cf: compass degrees from home plate toward center field
# Used for wind impact calculation (wind blowing out to CF = more HR)
VENUES = {
    # --- MLB Regular Season ---
    15: {"lat": 33.4453, "lon": -112.0667, "dome": True,  "alt_ft": 1086, "name": "Chase Field", "bearing": 0},
    680: {"lat": 33.8003, "lon": -84.3886, "dome": False, "alt_ft": 1050, "name": "Truist Park", "bearing": 170},
    2: {"lat": 39.2838, "lon": -76.6216, "dome": False, "alt_ft": 30,   "name": "Camden Yards", "bearing": 168},
    3: {"lat": 42.3467, "lon": -71.0972, "dome": False, "alt_ft": 20,   "name": "Fenway Park", "bearing": 210},
    17: {"lat": 41.9484, "lon": -87.6553, "dome": False, "alt_ft": 595,  "name": "Wrigley Field", "bearing": 210},
    4: {"lat": 41.8300, "lon": -87.6339, "dome": False, "alt_ft": 595,  "name": "Guaranteed Rate Field", "bearing": 180},
    2602: {"lat": 39.0974, "lon": -84.5082, "dome": False, "alt_ft": 490, "name": "Great American Ball Park", "bearing": 175},
    5: {"lat": 41.4962, "lon": -81.6852, "dome": False, "alt_ft": 660,  "name": "Progressive Field", "bearing": 175},
    19: {"lat": 39.7561, "lon": -104.9942, "dome": False, "alt_ft": 5280, "name": "Coors Field", "bearing": 170},
    2394: {"lat": 42.3390, "lon": -83.0485, "dome": False, "alt_ft": 585, "name": "Comerica Park", "bearing": 175},
    2392: {"lat": 29.7573, "lon": -95.3555, "dome": True,  "alt_ft": 50,  "name": "Minute Maid Park", "bearing": 0},
    7: {"lat": 39.0517, "lon": -94.4803, "dome": False, "alt_ft": 750,  "name": "Kauffman Stadium", "bearing": 180},
    22: {"lat": 33.8003, "lon": -118.2400, "dome": False, "alt_ft": 340, "name": "Angel Stadium", "bearing": 175},
    10: {"lat": 34.0739, "lon": -118.2400, "dome": False, "alt_ft": 515, "name": "Dodger Stadium", "bearing": 165},
    32: {"lat": 25.7781, "lon": -80.2196, "dome": True,  "alt_ft": 10,   "name": "loanDepot park", "bearing": 0},
    209: {"lat": 43.0280, "lon": -87.9712, "dome": True,  "alt_ft": 600, "name": "American Family Field", "bearing": 0},
    3312: {"lat": 44.9817, "lon": -93.2776, "dome": False, "alt_ft": 840, "name": "Target Field", "bearing": 165},
    3289: {"lat": 40.7571, "lon": -73.8458, "dome": False, "alt_ft": 20,  "name": "Citi Field", "bearing": 175},
    3313: {"lat": 40.8296, "lon": -73.9262, "dome": False, "alt_ft": 55,  "name": "Yankee Stadium", "bearing": 185},
    10: {"lat": 34.0739, "lon": -118.2400, "dome": False, "alt_ft": 515, "name": "Dodger Stadium", "bearing": 165},
    2856: {"lat": 37.7516, "lon": -122.2005, "dome": False, "alt_ft": 5,  "name": "Oakland Coliseum", "bearing": 175},
    2681: {"lat": 39.9061, "lon": -75.1665, "dome": False, "alt_ft": 20,  "name": "Citizens Bank Park", "bearing": 175},
    31: {"lat": 40.4469, "lon": -80.0058, "dome": False, "alt_ft": 730,  "name": "PNC Park", "bearing": 170},
    2680: {"lat": 32.7073, "lon": -117.1566, "dome": False, "alt_ft": 15, "name": "Petco Park", "bearing": 175},
    2395: {"lat": 37.7786, "lon": -122.3893, "dome": False, "alt_ft": 5,  "name": "Oracle Park", "bearing": 175},
    680: {"lat": 33.8003, "lon": -84.3886, "dome": False, "alt_ft": 1050, "name": "Truist Park", "bearing": 170},
    2889: {"lat": 47.5914, "lon": -122.3324, "dome": True,  "alt_ft": 20, "name": "T-Mobile Park", "bearing": 0},
    14: {"lat": 38.6226, "lon": -90.1928, "dome": False, "alt_ft": 455,  "name": "Busch Stadium", "bearing": 180},
    12: {"lat": 27.7682, "lon": -82.6534, "dome": True,  "alt_ft": 44,   "name": "Tropicana Field", "bearing": 0},
    5325: {"lat": 30.4083, "lon": -97.7522, "dome": True,  "alt_ft": 560, "name": "Globe Life Field", "bearing": 0},
    14: {"lat": 38.6226, "lon": -90.1928, "dome": False, "alt_ft": 455,  "name": "Busch Stadium", "bearing": 180},
    3309: {"lat": 43.6414, "lon": -79.3894, "dome": True,  "alt_ft": 260, "name": "Rogers Centre", "bearing": 0},
    3714: {"lat": 38.8730, "lon": -77.0075, "dome": False, "alt_ft": 25,  "name": "Nationals Park", "bearing": 175},

    # --- Spring Training (Cactus League) ---
    2500: {"lat": 33.4392, "lon": -111.8783, "dome": False, "alt_ft": 1200, "name": "Salt River Fields", "bearing": 175},
    4249: {"lat": 33.3838, "lon": -111.9674, "dome": False, "alt_ft": 1175, "name": "Tempe Diablo Stadium", "bearing": 175},
    2700: {"lat": 33.5264, "lon": -112.3848, "dome": False, "alt_ft": 960,  "name": "Camelback Ranch", "bearing": 175},
    3805: {"lat": 33.4375, "lon": -112.0008, "dome": False, "alt_ft": 1100, "name": "American Family Fields", "bearing": 175},
    3809: {"lat": 33.4631, "lon": -111.6267, "dome": False, "alt_ft": 1330, "name": "Sloan Park", "bearing": 175},
    3000: {"lat": 33.4456, "lon": -111.9131, "dome": False, "alt_ft": 1200, "name": "Scottsdale Stadium", "bearing": 175},
    2603: {"lat": 33.3200, "lon": -111.8922, "dome": False, "alt_ft": 1200, "name": "Goodyear Ballpark", "bearing": 175},
    2752: {"lat": 33.5183, "lon": -111.9253, "dome": False, "alt_ft": 1250, "name": "Peoria Sports Complex", "bearing": 175},
    3807: {"lat": 33.4378, "lon": -111.8325, "dome": False, "alt_ft": 1200, "name": "Hohokam Stadium", "bearing": 175},
    3806: {"lat": 33.5078, "lon": -112.2264, "dome": False, "alt_ft": 1050, "name": "Surprise Stadium", "bearing": 175},

    # --- Spring Training (Grapefruit League) ---
    2534: {"lat": 28.0747, "lon": -80.6489, "dome": False, "alt_ft": 25, "name": "Space Coast Stadium", "bearing": 175},
    2508: {"lat": 27.8961, "lon": -82.7833, "dome": False, "alt_ft": 5,  "name": "BayCare Ballpark", "bearing": 175},
    2507: {"lat": 26.3581, "lon": -80.0983, "dome": False, "alt_ft": 10, "name": "Roger Dean Chevrolet Stadium", "bearing": 175},
    2520: {"lat": 26.5350, "lon": -80.0850, "dome": False, "alt_ft": 15, "name": "The Ballpark of the Palm Beaches", "bearing": 175},
    2526: {"lat": 27.3494, "lon": -82.5042, "dome": False, "alt_ft": 10, "name": "Ed Smith Stadium", "bearing": 175},
    2518: {"lat": 26.3111, "lon": -80.1583, "dome": False, "alt_ft": 10, "name": "JetBlue Park", "bearing": 175},
    2536: {"lat": 28.0614, "lon": -82.7178, "dome": False, "alt_ft": 30, "name": "George M. Steinbrenner Field", "bearing": 175},
    2505: {"lat": 26.6167, "lon": -81.9431, "dome": False, "alt_ft": 10, "name": "CenturyLink Sports Complex", "bearing": 175},
    2504: {"lat": 28.5386, "lon": -81.4028, "dome": False, "alt_ft": 95, "name": "Champion Stadium", "bearing": 175},
    2523: {"lat": 27.3706, "lon": -80.3508, "dome": False, "alt_ft": 10, "name": "Clover Park", "bearing": 175},
    4629: {"lat": 28.3392, "lon": -81.5625, "dome": False, "alt_ft": 80, "name": "CoolToday Park", "bearing": 175},
    4309: {"lat": 27.0878, "lon": -82.0492, "dome": False, "alt_ft": 15, "name": "LECOM Park", "bearing": 175},
    5000: {"lat": 28.3392, "lon": -80.6106, "dome": False, "alt_ft": 20, "name": "FITTEAM Ballpark", "bearing": 175},
    2511: {"lat": 28.0603, "lon": -82.7164, "dome": False, "alt_ft": 30, "name": "TD Ballpark", "bearing": 175},
}

# Fallback for unknown venues
_DEFAULT = {"lat": 39.0, "lon": -95.0, "dome": False, "alt_ft": 500, "name": "Unknown", "bearing": 180}


def get_venue_meta(venue_id):
    """Return venue metadata dict, or a neutral default."""
    return VENUES.get(venue_id, _DEFAULT)
