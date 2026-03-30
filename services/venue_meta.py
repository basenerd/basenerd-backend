# services/venue_meta.py
"""MLB venue metadata: coordinates, dome status, altitude, orientation, dimensions."""

# venue_id → {lat, lon, dome, alt_ft, name, bearing, lf_dist, cf_dist, rf_dist,
#              lf_wall, cf_wall, rf_wall}
#
# bearing:  compass degrees from home plate toward center field (for wind calculation)
# *_dist:   fence distance in feet at the foul pole / straightaway CF
# *_wall:   wall height in feet (standard = 8 ft; notable exceptions listed)
#
# League averages used for dimension factor baseline:
#   LF ≈ 331 ft, CF ≈ 404 ft, RF ≈ 328 ft, all walls ≈ 8 ft
VENUES = {
    # --- American League East ---
    2: {   # Oriole Park at Camden Yards
        "lat": 39.2838, "lon": -76.6216, "dome": False, "alt_ft": 30,
        "name": "Oriole Park at Camden Yards", "bearing": 32,
        "lf_dist": 333, "cf_dist": 410, "rf_dist": 318,
        "lf_wall": 7,  "cf_wall": 7,  "rf_wall": 7,
    },
    3: {   # Fenway Park
        "lat": 42.3467, "lon": -71.0972, "dome": False, "alt_ft": 20,
        "name": "Fenway Park", "bearing": 45,
        "lf_dist": 310, "cf_dist": 420, "rf_dist": 302,
        "lf_wall": 37, "cf_wall": 17, "rf_wall": 3,   # Green Monster
    },
    3289: {  # Citi Field
        "lat": 40.7571, "lon": -73.8458, "dome": False, "alt_ft": 20,
        "name": "Citi Field", "bearing": 32,
        "lf_dist": 335, "cf_dist": 408, "rf_dist": 330,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    3313: {  # Yankee Stadium
        "lat": 40.8296, "lon": -73.9262, "dome": False, "alt_ft": 55,
        "name": "Yankee Stadium", "bearing": 88,
        "lf_dist": 318, "cf_dist": 408, "rf_dist": 314,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    12: {   # Tropicana Field (fixed dome)
        "lat": 27.7682, "lon": -82.6534, "dome": True, "alt_ft": 44,
        "name": "Tropicana Field", "bearing": 0,
        "lf_dist": 315, "cf_dist": 404, "rf_dist": 322,
        "lf_wall": 10, "cf_wall": 10, "rf_wall": 10,
    },
    14: {   # Rogers Centre (retractable)
        "lat": 43.6414, "lon": -79.3894, "dome": True, "retractable": True, "alt_ft": 260,
        "name": "Rogers Centre", "bearing": 315,
        "lf_dist": 328, "cf_dist": 400, "rf_dist": 328,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },

    # --- American League Central ---
    4: {   # Rate Field
        "lat": 41.8300, "lon": -87.6339, "dome": False, "alt_ft": 595,
        "name": "Rate Field", "bearing": 128,
        "lf_dist": 330, "cf_dist": 400, "rf_dist": 335,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    5: {   # Progressive Field
        "lat": 41.4962, "lon": -81.6852, "dome": False, "alt_ft": 660,
        "name": "Progressive Field", "bearing": 358,
        "lf_dist": 325, "cf_dist": 405, "rf_dist": 325,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    7: {   # Kauffman Stadium
        "lat": 39.0517, "lon": -94.4803, "dome": False, "alt_ft": 750,
        "name": "Kauffman Stadium", "bearing": 45,
        "lf_dist": 330, "cf_dist": 410, "rf_dist": 330,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    2394: {  # Comerica Park
        "lat": 42.3390, "lon": -83.0485, "dome": False, "alt_ft": 585,
        "name": "Comerica Park", "bearing": 151,
        "lf_dist": 345, "cf_dist": 420, "rf_dist": 330,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    3312: {  # Target Field
        "lat": 44.9817, "lon": -93.2776, "dome": False, "alt_ft": 840,
        "name": "Target Field", "bearing": 90,
        "lf_dist": 339, "cf_dist": 404, "rf_dist": 328,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },

    # --- American League West ---
    2392: {  # Daikin Park / Minute Maid Park (retractable)
        "lat": 29.7573, "lon": -95.3555, "dome": True, "retractable": True, "alt_ft": 50,
        "name": "Daikin Park", "bearing": 347,
        "lf_dist": 315, "cf_dist": 435, "rf_dist": 326,
        "lf_wall": 7,  "cf_wall": 8,  "rf_wall": 8,  # Crawford Boxes at 315
    },
    1: {   # Angel Stadium
        "lat": 33.8003, "lon": -118.2400, "dome": False, "alt_ft": 340,
        "name": "Angel Stadium", "bearing": 44,
        "lf_dist": 330, "cf_dist": 396, "rf_dist": 330,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    2529: {  # Sutter Health Park (Athletics)
        "lat": 38.5803, "lon": -121.5002, "dome": False, "alt_ft": 25,
        "name": "Sutter Health Park", "bearing": 35,
        "lf_dist": 330, "cf_dist": 400, "rf_dist": 325,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    680: {  # T-Mobile Park (retractable)
        "lat": 47.5914, "lon": -122.3324, "dome": True, "retractable": True, "alt_ft": 20,
        "name": "T-Mobile Park", "bearing": 50,
        "lf_dist": 331, "cf_dist": 401, "rf_dist": 326,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    5325: {  # Globe Life Field (retractable)
        "lat": 30.4083, "lon": -97.7522, "dome": True, "retractable": True, "alt_ft": 560,
        "name": "Globe Life Field", "bearing": 135,
        "lf_dist": 332, "cf_dist": 407, "rf_dist": 326,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },

    # --- National League East ---
    4705: {  # Truist Park
        "lat": 33.8003, "lon": -84.3886, "dome": False, "alt_ft": 1050,
        "name": "Truist Park", "bearing": 120,
        "lf_dist": 335, "cf_dist": 400, "rf_dist": 325,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    4169: {  # loanDepot park (retractable)
        "lat": 25.7781, "lon": -80.2196, "dome": True, "retractable": True, "alt_ft": 10,
        "name": "loanDepot park", "bearing": 30,
        "lf_dist": 344, "cf_dist": 404, "rf_dist": 335,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    2681: {  # Citizens Bank Park
        "lat": 39.9061, "lon": -75.1665, "dome": False, "alt_ft": 20,
        "name": "Citizens Bank Park", "bearing": 9,
        "lf_dist": 329, "cf_dist": 401, "rf_dist": 330,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    3309: {  # Nationals Park
        "lat": 38.8730, "lon": -77.0075, "dome": False, "alt_ft": 25,
        "name": "Nationals Park", "bearing": 29,
        "lf_dist": 336, "cf_dist": 403, "rf_dist": 335,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },

    # --- National League Central ---
    17: {   # Wrigley Field
        "lat": 41.9484, "lon": -87.6553, "dome": False, "alt_ft": 595,
        "name": "Wrigley Field", "bearing": 38,
        "lf_dist": 355, "cf_dist": 400, "rf_dist": 353,
        "lf_wall": 15, "cf_wall": 10, "rf_wall": 11,
    },
    2602: {  # Great American Ball Park
        "lat": 39.0974, "lon": -84.5082, "dome": False, "alt_ft": 490,
        "name": "Great American Ball Park", "bearing": 125,
        "lf_dist": 328, "cf_dist": 404, "rf_dist": 325,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    32: {   # American Family Field (retractable)
        "lat": 43.0280, "lon": -87.9712, "dome": True, "retractable": True, "alt_ft": 600,
        "name": "American Family Field", "bearing": 129,
        "lf_dist": 344, "cf_dist": 400, "rf_dist": 345,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    31: {   # PNC Park
        "lat": 40.4469, "lon": -80.0058, "dome": False, "alt_ft": 730,
        "name": "PNC Park", "bearing": 117,
        "lf_dist": 325, "cf_dist": 399, "rf_dist": 320,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    2889: {  # Busch Stadium
        "lat": 38.6226, "lon": -90.1928, "dome": False, "alt_ft": 455,
        "name": "Busch Stadium", "bearing": 63,
        "lf_dist": 336, "cf_dist": 400, "rf_dist": 335,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },

    # --- National League West ---
    15: {   # Chase Field (retractable)
        "lat": 33.4453, "lon": -112.0667, "dome": True, "retractable": True, "alt_ft": 1086,
        "name": "Chase Field", "bearing": 26,
        "lf_dist": 330, "cf_dist": 407, "rf_dist": 334,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    19: {   # Coors Field
        "lat": 39.7561, "lon": -104.9942, "dome": False, "alt_ft": 5280,
        "name": "Coors Field", "bearing": 4,
        "lf_dist": 347, "cf_dist": 415, "rf_dist": 350,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    22: {   # UNIQLO Field at Dodger Stadium
        "lat": 34.0739, "lon": -118.2400, "dome": False, "alt_ft": 515,
        "name": "Dodger Stadium", "bearing": 27,
        "lf_dist": 330, "cf_dist": 395, "rf_dist": 330,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    2680: {  # Petco Park
        "lat": 32.7073, "lon": -117.1566, "dome": False, "alt_ft": 15,
        "name": "Petco Park", "bearing": 359,
        "lf_dist": 334, "cf_dist": 396, "rf_dist": 322,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 8,
    },
    2395: {  # Oracle Park
        "lat": 37.7786, "lon": -122.3893, "dome": False, "alt_ft": 5,
        "name": "Oracle Park", "bearing": 85,
        "lf_dist": 339, "cf_dist": 399, "rf_dist": 309,
        "lf_wall": 8,  "cf_wall": 8,  "rf_wall": 24,  # brick RF wall
    },

    # --- Spring Training (Cactus League) ---
    2500: {"lat": 33.4392, "lon": -111.8783, "dome": False, "alt_ft": 1200, "name": "Salt River Fields",          "bearing": 175, "lf_dist": 335, "cf_dist": 400, "rf_dist": 335, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    4249: {"lat": 33.3838, "lon": -111.9674, "dome": False, "alt_ft": 1175, "name": "Tempe Diablo Stadium",        "bearing": 175, "lf_dist": 330, "cf_dist": 400, "rf_dist": 330, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2700: {"lat": 33.5264, "lon": -112.3848, "dome": False, "alt_ft": 960,  "name": "Camelback Ranch",             "bearing": 175, "lf_dist": 330, "cf_dist": 400, "rf_dist": 330, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    3805: {"lat": 33.4375, "lon": -112.0008, "dome": False, "alt_ft": 1100, "name": "American Family Fields",      "bearing": 175, "lf_dist": 335, "cf_dist": 400, "rf_dist": 335, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    3809: {"lat": 33.4631, "lon": -111.6267, "dome": False, "alt_ft": 1330, "name": "Sloan Park",                  "bearing": 175, "lf_dist": 355, "cf_dist": 400, "rf_dist": 353, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    3000: {"lat": 33.4456, "lon": -111.9131, "dome": False, "alt_ft": 1200, "name": "Scottsdale Stadium",          "bearing": 175, "lf_dist": 360, "cf_dist": 430, "rf_dist": 350, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2603: {"lat": 33.3200, "lon": -111.8922, "dome": False, "alt_ft": 1200, "name": "Goodyear Ballpark",           "bearing": 175, "lf_dist": 335, "cf_dist": 400, "rf_dist": 335, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2752: {"lat": 33.5183, "lon": -111.9253, "dome": False, "alt_ft": 1250, "name": "Peoria Sports Complex",       "bearing": 175, "lf_dist": 340, "cf_dist": 400, "rf_dist": 340, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    3807: {"lat": 33.4378, "lon": -111.8325, "dome": False, "alt_ft": 1200, "name": "Hohokam Stadium",             "bearing": 175, "lf_dist": 340, "cf_dist": 410, "rf_dist": 340, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    3806: {"lat": 33.5078, "lon": -112.2264, "dome": False, "alt_ft": 1050, "name": "Surprise Stadium",            "bearing": 175, "lf_dist": 335, "cf_dist": 400, "rf_dist": 335, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},

    # --- Spring Training (Grapefruit League) ---
    2534: {"lat": 28.0747, "lon": -80.6489, "dome": False, "alt_ft": 25, "name": "Space Coast Stadium",            "bearing": 175, "lf_dist": 330, "cf_dist": 400, "rf_dist": 325, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2508: {"lat": 27.8961, "lon": -82.7833, "dome": False, "alt_ft": 5,  "name": "BayCare Ballpark",               "bearing": 175, "lf_dist": 330, "cf_dist": 400, "rf_dist": 330, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2520: {"lat": 26.5350, "lon": -80.0850, "dome": False, "alt_ft": 15, "name": "The Ballpark of the Palm Beaches","bearing": 175, "lf_dist": 325, "cf_dist": 400, "rf_dist": 325, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2526: {"lat": 27.3494, "lon": -82.5042, "dome": False, "alt_ft": 10, "name": "Ed Smith Stadium",               "bearing": 175, "lf_dist": 330, "cf_dist": 400, "rf_dist": 330, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2518: {"lat": 26.3111, "lon": -80.1583, "dome": False, "alt_ft": 10, "name": "JetBlue Park",                   "bearing": 175, "lf_dist": 310, "cf_dist": 420, "rf_dist": 302, "lf_wall": 37, "cf_wall": 17, "rf_wall": 3},  # replica Fenway
    2536: {"lat": 28.0614, "lon": -82.7178, "dome": False, "alt_ft": 30, "name": "George M. Steinbrenner Field",   "bearing": 175, "lf_dist": 318, "cf_dist": 408, "rf_dist": 314, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2505: {"lat": 26.6167, "lon": -81.9431, "dome": False, "alt_ft": 10, "name": "CenturyLink Sports Complex",     "bearing": 175, "lf_dist": 330, "cf_dist": 400, "rf_dist": 330, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2504: {"lat": 28.5386, "lon": -81.4028, "dome": False, "alt_ft": 95, "name": "Champion Stadium",               "bearing": 175, "lf_dist": 340, "cf_dist": 407, "rf_dist": 340, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2523: {"lat": 27.3706, "lon": -80.3508, "dome": False, "alt_ft": 10, "name": "Clover Park",                    "bearing": 175, "lf_dist": 335, "cf_dist": 403, "rf_dist": 335, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    4629: {"lat": 28.3392, "lon": -81.5625, "dome": False, "alt_ft": 80, "name": "CoolToday Park",                 "bearing": 175, "lf_dist": 335, "cf_dist": 412, "rf_dist": 335, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    4309: {"lat": 27.0878, "lon": -82.0492, "dome": False, "alt_ft": 15, "name": "LECOM Park",                     "bearing": 175, "lf_dist": 335, "cf_dist": 400, "rf_dist": 335, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    5000: {"lat": 28.3392, "lon": -80.6106, "dome": False, "alt_ft": 20, "name": "FITTEAM Ballpark",               "bearing": 175, "lf_dist": 325, "cf_dist": 400, "rf_dist": 325, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
    2511: {"lat": 28.0603, "lon": -82.7164, "dome": False, "alt_ft": 30, "name": "TD Ballpark",                    "bearing": 175, "lf_dist": 330, "cf_dist": 400, "rf_dist": 330, "lf_wall": 8, "cf_wall": 8, "rf_wall": 8},
}

# Fallback for unknown venues
_DEFAULT = {
    "lat": 39.0, "lon": -95.0, "dome": False, "alt_ft": 500,
    "name": "Unknown", "bearing": 45,
    "lf_dist": 331, "cf_dist": 404, "rf_dist": 328,
    "lf_wall": 8, "cf_wall": 8, "rf_wall": 8,
}


def get_venue_meta(venue_id):
    """Return venue metadata dict, or a neutral default."""
    return VENUES.get(venue_id, _DEFAULT)
