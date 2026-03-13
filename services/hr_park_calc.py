"""
HR Park Calculator
==================
Determines which MLB stadiums a batted ball would clear the fence in.

Uses physics-based trajectory simulation (drag + lift) with per-stadium
elevation/temperature adjustments and precise piecewise outfield fence profiles.

Usage:
    from services.hr_park_calc import stadiums_hr_count
    result = stadiums_hr_count(exit_velo=105, launch_angle=28, spray_angle=-10)
    # result = {count: 22, total: 30, parks: [{team, name, is_hr, margin_ft}, ...]}
"""

import math
from typing import List, Dict, Optional, Tuple

# ---------------------------------------------------------------------------
#  Physics constants
# ---------------------------------------------------------------------------
BALL_MASS = 0.1450        # kg (5.125 oz)
BALL_RADIUS = 0.03639     # m
BALL_AREA = math.pi * BALL_RADIUS ** 2
GRAVITY = 9.80665         # m/s^2
FT_TO_M = 0.3048
MPH_TO_MS = 0.44704
CONTACT_HEIGHT_FT = 3.0   # bat contact height
LIFT_COEFF = 0.22         # constant CL for batted-ball backspin

# ---------------------------------------------------------------------------
#  Stadium data
#  Angles: degrees, 0 = LF foul pole, 90 = RF foul pole
#  Distance segments (format 0): (c0, c1, theta_min, theta_max)
#      r = c0 / (sin(θ_rad) + c1 * cos(θ_rad))
#  Distance segments (format 1 / elliptical):
#      ([c0..c6], [c7, c8], theta_min, theta_max)
#  Distance segments (format 2 / string):
#      (c0, 'sin'|'cos'|'con', theta_min, theta_max)
#  Height segments: (h, slope_flag, theta_min, theta_max)
#      slope_flag=0: constant h; slope_flag=1: linear interp to next segment's h
# ---------------------------------------------------------------------------

_STADIUMS = {
    "AZ": {
        "name": "Chase Field",
        "elevation": 1086, "temp": 80.8,
        "dist": [
            (-389.4194, -1.1624467, 0, 4.9), (423.5471, 1.085346, 4.9, 6.6),
            (6211.3885, 17.49789, 6.6, 31.7), (559.10919, 1.0073058, 31.7, 38.9),
            (-91.557622, -1.10398598, 38.9, 39.1), (571.92441, 1.0070058, 39.1, 50.5),
            (114.59269, -0.76826977, 50.5, 50.8), (557.962, 1.0031979, 50.8, 57.7),
            (353.793768, 0.06108017, 57.7, 82.5), (395.0241, 0.9533913, 82.5, 84.2),
            (327, -0.9060869, 84.2, 90),
        ],
        "height": [
            (8, 0, 0, 4.9), (8, 1, 4.9, 6.6), (7.5, 0, 6.6, 31.7),
            (25, 0, 31.7, 57.7), (7.5, 0, 57.7, 82.5), (7.5, 1, 82.5, 84.2),
            (8, 0, 84.2, 90),
        ],
    },
    "ATL": {
        "name": "Truist Park",
        "elevation": 1001, "temp": 79.2,
        "dist": [
            (2543.7, 7.593, 0, 15), (-3393.3, -10.752, 15, 30),
            (788.1, 1.786, 30, 45), (407.2, 0.4397, 45, 60),
            (305.0, -0.1054, 60, 75), (325.06, 0.1324, 75, 90),
        ],
        "height": [
            (11, 0, 0, 20), (15, 0, 20, 40), (8, 0, 40, 90),
        ],
    },
    "BAL": {
        "name": "Oriole Park at Camden Yards",
        "elevation": 33, "temp": 76.4,
        "dist": [
            (-1789.977, -5.61943, 0, 25.5), (801.702, 1.83, 25.5, 49),
            (359.7761, 0.187168, 49, 82), (331, -0.396914, 82, 90),
        ],
        "height": [(21, 0, 0, 16.2), (7, 0, 16.2, 90)],
    },
    "BOS": {
        "name": "Fenway Park",
        "elevation": 21, "temp": 69.5,
        "dist": [
            (-119.0423, -0.3941798, 0, 3.8), (-402.289, -1.17404, 3.8, 4.9),
            (-808.953, -2.274195, 4.9, 6), (-2332.79083, -6.3601456, 6, 7.1),
            (-20759.85313, -55.616, 7.1, 8.1), (1129.33168, 2.875435, 8.1, 31),
            (-417.143116, -1.8849057, 31, 33.8), (431.2604, 0.587157, 33.8, 52.2),
            (2077.8716, 7.7513156, 52.2, 53.1), (306, 0.00577087, 53.1, 90),
        ],
        "height": [
            (5, 0, 0, 4.9), (5, 1, 4.9, 6), (3, 0, 6, 7.1), (3, 0, 7.1, 8.1),
            (5, 0, 8.1, 31), (5, 0, 31, 33.8), (18, 0, 33.8, 53.1), (37, 0, 53.1, 90),
        ],
    },
    "CHC": {
        "name": "Wrigley Field",
        "elevation": 595, "temp": 70.2,
        "dist": [
            (-4499.413, -12.7462, 0, 10.9), (297.1748, 0.636566, 10.9, 13.1),
            (18363.859, 53.4839, 13.1, 29.4),
            ([9353823.75, 33.2, 2540504.25, 146.8, 33526.25, 9105.75, 180], [22815.51, 155682], 29.4, 49.2),
            (357.8732, 0.245827, 49.2, 73.2), (496.86435, 1.62768, 73.2, 74.8),
            (355, 0.112061, 74.8, 90),
        ],
        "height": [
            (15, 0, 0, 10.9), (15, 1, 10.9, 13.1), (11.5, 0, 13.1, 73.2),
            (11.5, 1, 73.2, 74.8), (15, 0, 74.8, 90),
        ],
    },
    "CIN": {
        "name": "Great American Ball Park",
        "elevation": 535, "temp": 77.9,
        "dist": [
            ([11951552.5, 25.2, 8986447.5, 164.8, 41212.25, 30987.75, 190], [19212.09, 168200], 0, 44.7),
            (436.311, 0.52231577, 44.7, 60.3), (336.435, 0.0014347, 60.3, 86.6),
            (326, -0.5206991, 86.6, 90),
        ],
        "height": [(12, 0, 0, 1), (8, 0, 1, 44.7), (12, 0, 44.7, 90)],
    },
    "CLE": {
        "name": "Progressive Field",
        "elevation": 653, "temp": 70.8,
        "dist": [
            (-1609.844, -4.98404, 0, 20.3), (906.183, 2.2274, 20.3, 48.2),
            (356.7465, 0.197554, 48.2, 78.2), (321, -0.303978, 78.2, 90),
        ],
        "height": [(14, 0, 0, 2), (8, 0, 2, 48.2), (19, 0, 48.2, 90)],
    },
    "COL": {
        "name": "Coors Field",
        "elevation": 5190, "temp": 75.4,
        "dist": [
            (-551.417, -1.57548, 0, 1.2), (4061.537, 11.422, 1.2, 37.5),
            (536.536, 0.84288, 37.5, 60.2), (345, -0.08135, 60.2, 90),
        ],
        "height": [(17, 0, 0, 17.3), (8, 0, 17.3, 90)],
    },
    "CWS": {
        "name": "Guaranteed Rate Field",
        "elevation": 595, "temp": 71.6,
        "dist": [
            (-7014.6043, -20.939117, 0, 24.1), (1495.6997, 3.92207, 24.1, 30.6),
            (820.61, 1.88324, 30.6, 36.6), (1969.1759, 5.562717, 36.6, 39.1),
            (561.4967, 1.00525, 39.1, 50.6), (363.2118, 0.2203438, 50.6, 54),
            (426.18439, 0.49718, 54, 58.7), (378.8179, 0.259128, 58.7, 63.4),
            (340.82399, 0.03285, 63.4, 79), (327, -0.177146, 79, 90),
        ],
        "height": [(8, 0, 0, 90)],
    },
    "DET": {
        "name": "Comerica Park",
        "elevation": 600, "temp": 73.3,
        "dist": [
            (-410, -1.23813, 0, 1.3), (337.21, 'cos', 1.3, 22.5),
            (-430.4868, -1.6908, 22.5, 24.6), (347.675, 'cos', 24.6, 35.3),
            (593.97, 1, 35.3, 54), (345, 'sin', 54, 90),
        ],
        "height": [
            (9, 0, 0, 22.5), (9, 1, 22.5, 24.6), (15, 0, 24.6, 35.3),
            (9, 0, 35.3, 54), (7, 0, 54, 90),
        ],
    },
    "HOU": {
        "name": "Minute Maid Park",
        "elevation": 45, "temp": 73.0,
        "dist": [
            (-2738.7177, -8.400974, 0, 23), (315.172, 0.493462, 23, 24.1),
            (-2943.702, -9.23423, 24.1, 35.2), (522, 0.81, 35.2, 51.2),
            (347.579, 0.120368, 51.2, 67.7), (42.673422, -2.124119, 67.7, 67.9),
            (315, 0.0366002, 67.9, 90),
        ],
        "height": [
            (7, 0, 0, 23), (9, 0, 23, 53.6), (25, 0, 53.6, 67.7),
            (25, 1, 67.7, 67.9), (21, 0, 67.9, 90),
        ],
    },
    "KC": {
        "name": "Kauffman Stadium",
        "elevation": 856, "temp": 77.4,
        "dist": [
            ([1738857, 10.1, 495945, 169.9, 5417, 1545, 180], [3671.3, 206082], 0, 5.9),
            (25650.376, 71.503534, 5.9, 22.1),
            ([19759218, 50.9, 1837968, -76.9, 111634, 10384, -26], [78594.9, 62658], 22.1, 59),
            ([5643864, 68.7, 4885920, -80.7, 16218, 14040, -12], [5740.3, 242208], 59, 76.9),
            (361.884, 0.01803985, 76.9, 82.7),
            ([958907, 82.6, 322725, -44.6, 2897, 975, 38], [1929, 219122], 82.7, 90),
        ],
        "height": [(8, 0, 0, 90)],
    },
    "LAA": {
        "name": "Angel Stadium",
        "elevation": 151, "temp": 76.8,
        "dist": [
            (-352.2388, -1.06739, 0, 1.6), (-496.7696, -1.493901, 1.6, 3.2),
            (-641.02615, -1.9114787, 3.2, 4.8), (-1020.3203, -2.9928111, 4.8, 6.6),
            (6919.533, 19.3875915, 6.6, 11.2), (1240.50705, 3.314747, 11.2, 42.6),
            (437.37565, 0.5733725, 42.6, 68), (351.0005, -0.0286525, 68, 84),
            (340.789, -0.3046164, 84, 85.6), (329.5441, -0.72339596, 85.6, 87),
            (324.50638, -1.0040283, 87, 88.4), (328, -0.629411, 88.4, 90),
        ],
        "height": [(5, 0, 0, 6.6), (8, 0, 6.6, 84), (5, 0, 84, 90)],
    },
    "LAD": {
        "name": "Dodger Stadium",
        "elevation": 515, "temp": 74.0,
        "dist": [
            (-443.8081, -1.344873, 0, 4.2), (-829.5118, -2.44985, 4.2, 7.8),
            (-10942.3745, -30.646819, 7.8, 9.5), (1719.756, 4.622957, 9.5, 25.1),
            (1115.073, 2.83277, 25.1, 31.1), (928.868, 2.258998, 31.1, 42.6),
            (742.26267, 1.620443, 42.6, 44), (562.6864, 0.9947777, 44, 46.3),
            (472.8006, 0.66870534, 46.3, 49.2), (423.6147, 0.478618, 49.2, 55.3),
            (395.11776, 0.349269, 55.3, 59), (392.2193, 0.3344991, 59, 63.1),
            (381.7462, 0.2729345, 63.1, 69.2), (372.8431, 0.2051737, 69.2, 74.7),
            (368.8506, 0.163833, 74.7, 80.5), (362.23, 0.053704, 80.5, 82.1),
            (353.007, -0.131245, 82.1, 83.3), (334.774, -0.564136, 83.3, 85.6),
            (333.006, -0.629807, 85.6, 87.2), (328.317, -0.90885, 87.2, 88.4),
            (330, -0.729958, 88.4, 90),
        ],
        "height": [
            (4, 0, 0, 7.8), (4, 1, 7.8, 10.6), (8, 0, 10.6, 80.5),
            (8, 1, 80.5, 83.3), (4, 0, 83.3, 90),
        ],
    },
    "MIA": {
        "name": "loanDepot Park",
        "elevation": 10, "temp": 72.2,
        "dist": [
            (-3285.092, -9.80624, 0, 23.7), (5130.955, 14.1917, 23.7, 27.1),
            (1260.2215, 3.099602, 27.1, 33.1), (928.6866, 2.112672, 33.1, 37.2),
            (822.69397, 1.78492, 37.2, 41.2), (697.69, 1.380693, 41.2, 44.6),
            (624.24997, 1.1315557, 44.6, 47.9), (567.2531, 0.927191, 47.9, 51.2),
            (123.3194, -0.7717922, 51.2, 51.3), (163.1114, -0.618006, 51.3, 51.6),
            (263.849, -0.2205658, 51.6, 52.3), (333.1776, 0.061447, 52.3, 53.6),
            (333.94836, 0.06472688, 53.6, 55), (391.363, 0.32139203, 55, 56.5),
            (457.485, 0.630953, 56.5, 59), (389.587, 0.2903055, 59, 60.8),
            (387.8902, 0.281246, 60.8, 63.6), (367.932, 0.163124, 63.6, 68.2),
            (360.9411, 0.112519, 68.2, 72.1), (349.332, 0.00931917, 72.1, 79.2),
            (339.562, -0.137541, 79.2, 84.3), (337, -0.212114, 84.3, 90),
        ],
        "height": [
            (8.5, 0, 0, 9.8), (11.5, 0, 9.8, 16), (8.5, 0, 16, 53.3),
            (11.5, 0, 53.3, 65.1), (7, 0, 65.1, 84.9), (11.5, 0, 84.9, 90),
        ],
    },
    "MIL": {
        "name": "American Family Field",
        "elevation": 597, "temp": 73.4,
        "dist": [
            (4068.1011, 11.7916, 0, 16.5), (-60.8626, -0.47706, 16.5, 16.8),
            (3834.475, 10.73232, 16.8, 23.3), (1042.985, 2.60569, 23.3, 35.5),
            (-1107.4106, -4.237288, 35.5, 37.7), (566.71123, 1, 37.7, 52.3),
            (287.52, -0.130068, 52.3, 56.2), (393.82239, 0.374126, 56.2, 74),
            (358.50448, 0.027826, 74, 85), (344, -0.435742, 85, 90),
        ],
        "height": [(8, 0, 0, 90)],
    },
    "MIN": {
        "name": "Target Field",
        "elevation": 815, "temp": 71.2,
        "dist": [
            (-2731.998, -8.3292, 0, 20), (1691.285, 4.5671, 20, 38.5),
            (629.3765, 1.2001, 38.5, 51.2), (382.741, 0.24243, 51.2, 67),
            (339, -0.05651, 67, 90),
        ],
        "height": [(23, 0, 0, 38.5), (8, 0, 38.5, 90)],
    },
    "NYM": {
        "name": "Citi Field",
        "elevation": 10, "temp": 73.9,
        "dist": [
            (-2766.825, -8.3843195, 0, 5.2), (-371.523921, -1.204617, 5.2, 7),
            (-1855.73071, -5.52645, 7, 18.8), (682.2307, 1.566132, 18.8, 23.3),
            (-40721.387, -119.6616683, 23.3, 29.5), (1281.67692, 3.1812751, 29.5, 38.2),
            (575.86589, 0.9960149, 38.2, 49.1), (358.6125, 0.1847292, 49.1, 82.1),
            (335, -0.30194697, 82.1, 90),
        ],
        "height": [(8, 0, 0, 90)],
    },
    "NYY": {
        "name": "Yankee Stadium",
        "elevation": 55, "temp": 72.8,
        "dist": [
            (-752.7416, -2.397266, 0, 3.2), (-1341.4764, -4.22849, 3.2, 4.9),
            (323.639, 'cos', 4.9, 30.6), (2683.6147, 7.700602, 30.6, 36.1),
            (913.27186, 2.139572, 36.1, 40.4), (707.36801, 1.4643105, 40.4, 44.4),
            (600.6388, 1.096466, 44.4, 48.4), (496.311752, 0.7103813, 48.4, 52.1),
            (445.2994, 0.5053365, 52.1, 56.7), (390.30014, 0.2548946, 56.7, 62.8),
            (345.39856, 0.001719809, 62.8, 80.6), (324.4985, -0.3638949, 80.6, 84.8),
            (316, -0.6421425, 84.8, 90),
        ],
        "height": [(8, 0, 0, 90)],
    },
    "OAK": {
        "name": "Oakland Coliseum",
        "elevation": 3, "temp": 70.6,
        "dist": [
            (-868.87639, -2.632959, 0, 12), (911.9339, 2.32779, 12, 22.3),
            (1753.07386, 4.853163, 22.3, 33.8), (1043.552, 2.617999, 33.8, 40.8),
            (558.5246, 1, 40.8, 49.2), (398.6068, 0.381971, 49.2, 56.2),
            (361.22296, 0.2060512, 56.2, 67.7), (391.759586, 0.429592, 67.7, 78),
            (330, -0.3798, 78, 90),
        ],
        "height": [
            (8, 0, 0, 12), (15, 0, 12, 33.8), (8, 0, 33.8, 56.2),
            (15, 0, 56.2, 78), (8, 0, 78, 90),
        ],
    },
    "PHI": {
        "name": "Citizens Bank Park",
        "elevation": 20, "temp": 76.6,
        "dist": [
            (330, 'cos', 0, 34.3), (644.15, 1.277017, 34.3, 50.7),
            (308.591, -0.02468, 50.7, 55.9), (543.4657, 1.08071, 55.9, 59.3),
            (331, 'sin', 59.3, 88.3), (325, -0.596191, 88.3, 90),
        ],
        "height": [
            (13, 0, 0, 34.3), (6, 0, 34.3, 50.7), (19, 0, 50.7, 53.2),
            (19, 1, 53.2, 55.9), (11, 0, 55.9, 90),
        ],
    },
    "PIT": {
        "name": "PNC Park",
        "elevation": 780, "temp": 73.9,
        "dist": [
            (-1759.947, -5.4827, 0, 22.3), (1120.146, 2.8184, 22.3, 34.1),
            (716.884, 1.56, 34.1, 44.3), (478.809, 0.71785, 44.3, 58.5),
            (-4560.837, -24.0136, 58.5, 59.6), (366.846, 0.089958, 59.6, 81.5),
            (321, -0.75751, 81.5, 90),
        ],
        "height": [(21, 0, 0, 22.3), (10, 0, 22.3, 59.6), (6, 0, 59.6, 90)],
    },
    "SD": {
        "name": "Petco Park",
        "elevation": 23, "temp": 71.7,
        "dist": [
            (321.433, 'cos', 0, 3.4), (-311.7359, -1.029242, 3.4, 7.2),
            (346.87116, 'cos', 7.2, 27.8), (1425.7353, 3.59492, 27.8, 31.8),
            (740.2202, 1.568309, 31.8, 38.3), (543.05468, 0.9402139, 38.3, 49.2),
            (318.3662, 0.0718681, 49.2, 50.4), (539.44852, 0.9611939, 50.4, 56.2),
            (393.566469, 0.2972994, 56.2, 63.5), (344.316, 0.0091096, 63.5, 83.8),
            (336, -0.2134522, 83.8, 90),
        ],
        "height": [(11, 1, 0, 3.4), (8, 0, 3.4, 90)],
    },
    "SEA": {
        "name": "T-Mobile Park",
        "elevation": 10, "temp": 63.8,
        "dist": [
            (-3502.437, -10.74367, 0, 26.5), (825.224, 1.9153, 26.5, 47),
            (414.291, 0.427476, 47, 59.6), (377.4922, 0.2382, 59.6, 66.5),
            (336.559, -0.037016, 66.5, 88.5), (331, -0.6671, 88.5, 90),
        ],
        "height": [(8, 0, 0, 90)],
    },
    "SF": {
        "name": "Oracle Park",
        "elevation": 0, "temp": 64.4,
        "dist": [
            (-697.339, -2.25676, 0, 15), (946.0859, 2.4155, 15, 18),
            (-712.5915, -2.389, 18, 26), (565, 1.025, 26, 55),
            (347.526, 0.07905, 55, 86.5), (335, -0.513097, 86.5, 90),
        ],
        "height": [
            (25, 0, 0, 15), (25, 1, 15, 18), (20, 0, 18, 26), (8, 0, 26, 90),
        ],
    },
    "STL": {
        "name": "Busch Stadium",
        "elevation": 460, "temp": 79.7,
        "dist": [
            (-436.689, -1.3173, 0, 3.3), (346.303, 'cos', 3.3, 25.6),
            (857.076, 1.995805, 25.6, 39.9), (569.534, 1.04571, 39.9, 50),
            (434.192, 0.514, 50, 64), (346.76, 'sin', 64, 88.4),
            (330, -1.73033, 88.4, 90),
        ],
        "height": [(8, 0, 0, 90)],
    },
    "TB": {
        "name": "Tropicana Field",
        "elevation": 15, "temp": 72.0,
        "dist": [
            (-357.101, -1.109, 0, 1.7), (331, 'cos', 1.7, 33.7),
            (1678.156, 4.403, 33.7, 36.2), (596.756, 1.09406, 36.2, 55),
            (275.103, -0.2654, 55, 56.4), (477.013, 0.6444, 56.4, 58),
            (342.43, 0.011121, 58, 86), (315, -1.13533, 86, 90),
        ],
        "height": [
            (9, 0, 0, 1.7), (11.5, 0, 1.7, 33.7), (9, 0, 33.7, 58),
            (11.5, 0, 58, 86), (5, 0, 86, 90),
        ],
    },
    "TEX": {
        "name": "Globe Life Field",
        "elevation": 545, "temp": 75.2,
        "dist": [
            (-3179.5, -9.664, 0, 22.5), (1036.4, 2.602, 22.5, 45),
            (402.4, 0.3983, 45, 67.5), (326.1, -0.1357, 67.5, 90),
        ],
        "height": [
            (8, 0, 0, 30), (14, 0, 30, 50), (8, 0, 50, 90),
        ],
    },
    "TOR": {
        "name": "Rogers Centre",
        "elevation": 270, "temp": 70.2,
        "dist": [
            (-1725.1974, -5.2597, 0, 20), (2160.354, 5.7667, 20, 32.5),
            (400, 'con', 32.5, 57.5), (374.6429, 0.17341, 57.5, 70),
            (328, -0.19012, 70, 90),
        ],
        "height": [(10, 0, 0, 90)],
    },
    "WSH": {
        "name": "Nationals Park",
        "elevation": 35, "temp": 76.5,
        "dist": [
            (-1192.9, -3.56091, 0, 13.1), (1018.837, 2.609847, 13.1, 46.5),
            (372.8599, 0.286983, 46.5, 57.9), (1089.6378, 3.903208, 57.9, 59),
            (383.87617, 0.297133, 59, 74.1), (163.401, -1.88975, 74.1, 74.2),
            (377.1893, 0.261412, 74.2, 76.5), (336, -0.221987, 76.5, 90),
        ],
        "height": [
            (16, 0, 0, 2), (9, 0, 2, 13.1), (14, 0, 13.1, 40),
            (9, 0, 40, 46.8), (10, 0, 46.8, 59), (9, 0, 59, 90),
        ],
    },
}


# ---------------------------------------------------------------------------
#  Fence evaluation helpers
# ---------------------------------------------------------------------------

def _eval_fence_distance(segments, theta_deg: float) -> float:
    """Return fence distance (ft) at angle theta_deg from LF pole."""
    theta_deg = max(0.1, min(89.9, theta_deg))
    theta_rad = math.radians(theta_deg)
    for seg in segments:
        t_min = seg[-2]
        t_max = seg[-1]
        if theta_deg < t_min or theta_deg > t_max:
            continue
        # Format 2: string-based
        if isinstance(seg[1], str):
            if seg[1] == 'cos':
                return seg[0] / math.cos(theta_rad)
            elif seg[1] == 'sin':
                return seg[0] / math.sin(theta_rad)
            else:  # 'con'
                return seg[0]
        # Format 1: elliptical (first element is a list)
        if isinstance(seg[0], list):
            c = seg[0]
            d = seg[1]
            den = c[4] - c[5] * math.cos(2 * theta_rad - math.radians(c[6]))
            m1 = (c[0] * math.cos(theta_rad - math.radians(c[1]))
                   - c[2] * math.cos(theta_rad - math.radians(c[3]))) / den
            s = math.sin(theta_rad - math.radians(c[1])) ** 2
            m2 = d[0] * math.sqrt(abs(den - d[1] * s)) / den
            return m1 + m2
        # Format 0: standard
        return seg[0] / (math.sin(theta_rad) + seg[1] * math.cos(theta_rad))
    # Fallback: use closest segment
    if theta_deg <= segments[0][-2]:
        return _eval_fence_distance(segments, segments[0][-2] + 0.1)
    return _eval_fence_distance(segments, segments[-1][-1] - 0.1)


def _eval_fence_height(segments, theta_deg: float) -> float:
    """Return fence height (ft) at angle theta_deg from LF pole."""
    theta_deg = max(0.1, min(89.9, theta_deg))
    for i, seg in enumerate(segments):
        h, slope, t_min, t_max = seg
        if theta_deg < t_min or theta_deg > t_max:
            continue
        if slope == 0:
            return h
        # Linearly interpolate to next segment's starting height
        if i + 1 < len(segments):
            h_next = segments[i + 1][0]
        else:
            h_next = h
        frac = (theta_deg - t_min) / (t_max - t_min) if t_max > t_min else 0
        return h + frac * (h_next - h)
    return 8.0  # default fallback


# ---------------------------------------------------------------------------
#  Physics / trajectory simulation
# ---------------------------------------------------------------------------

def _air_density(elevation_ft: float, temp_f: float = 72.0) -> float:
    """Air density (kg/m^3) from barometric formula."""
    elev_m = elevation_ft * FT_TO_M
    temp_k = (temp_f - 32) * 5 / 9 + 273.15
    P0, L, T0 = 101325.0, 0.0065, 288.15
    g, M, R = 9.80665, 0.0289644, 8.31447
    P = P0 * (1 - L * elev_m / T0) ** (g * M / (R * L))
    Rd = 287.058
    return P / (Rd * temp_k)


def _drag_coeff(v: float) -> float:
    """Adair velocity-dependent drag coefficient. v in m/s."""
    return 0.29 + 0.22 / (1 + math.exp((v - 32.37) / 5.2))


def _simulate_trajectory(
    ev_mph: float,
    la_deg: float,
    elevation_ft: float = 0,
    temp_f: float = 72.0,
) -> List[Tuple[float, float]]:
    """
    2D trajectory simulation: horizontal distance (ft) vs height (ft).

    Returns list of (horiz_dist_ft, height_ft) sampled every ~1.5 ft of
    horizontal travel.  The simulation uses Euler integration with drag and
    lift (backspin).
    """
    rho = _air_density(elevation_ft, temp_f)
    ev = ev_mph * MPH_TO_MS
    la = math.radians(la_deg)

    # Initial velocity components (2D: horizontal + vertical)
    vx = ev * math.cos(la)  # horizontal (toward outfield)
    vz = ev * math.sin(la)  # vertical (up)

    x = 0.0  # horizontal distance (m)
    z = CONTACT_HEIGHT_FT * FT_TO_M  # start at bat-contact height

    dt = 0.005  # 5ms timestep
    pts = [(0.0, CONTACT_HEIGHT_FT)]
    cl = LIFT_COEFF

    for _ in range(3000):  # max ~15 seconds
        v = math.sqrt(vx * vx + vz * vz)
        if v < 0.5:
            break

        cd = _drag_coeff(v)
        q = 0.5 * rho * BALL_AREA * v * v  # dynamic pressure * area

        # Drag (opposes velocity)
        fd = cd * q
        ax_drag = -fd * vx / (v * BALL_MASS)
        az_drag = -fd * vz / (v * BALL_MASS)

        # Lift from backspin (perpendicular to velocity, in the vertical plane)
        fl = cl * q
        ax_lift = -fl * vz / (v * BALL_MASS)
        az_lift = fl * vx / (v * BALL_MASS)

        # Euler step
        vx += (ax_drag + ax_lift) * dt
        vz += (-GRAVITY + az_drag + az_lift) * dt
        x += vx * dt
        z += vz * dt

        pts.append((x / FT_TO_M, z / FT_TO_M))

        # Ball hit the ground
        if z < 0 and x > 1:
            # Interpolate to ground level
            x_prev, z_prev = pts[-2][0] * FT_TO_M, pts[-2][1] * FT_TO_M
            if z_prev > 0:
                frac = z_prev / (z_prev - z)
                x_ground = (x_prev + frac * (x / FT_TO_M * FT_TO_M - x_prev)) / FT_TO_M
                pts[-1] = (x_ground, 0.0)
            break

    return pts


def _ball_height_at_distance(
    trajectory: List[Tuple[float, float]], fence_dist: float
) -> Optional[float]:
    """
    Interpolate the ball's height at a given horizontal distance.
    Returns None if the ball never reaches that distance.
    """
    for i in range(1, len(trajectory)):
        d0, h0 = trajectory[i - 1]
        d1, h1 = trajectory[i]
        if d0 <= fence_dist <= d1:
            if d1 == d0:
                return h1
            frac = (fence_dist - d0) / (d1 - d0)
            return h0 + frac * (h1 - h0)
    return None  # ball didn't reach the fence


# ---------------------------------------------------------------------------
#  Public API
# ---------------------------------------------------------------------------

def stadiums_hr_count(
    exit_velo: float,
    launch_angle: float,
    spray_angle: float,
) -> Dict:
    """
    Determine which MLB stadiums a batted ball would be a home run in.

    Args:
        exit_velo:    mph
        launch_angle: degrees
        spray_angle:  Statcast convention (0=CF, positive=RF, negative=LF)

    Returns:
        dict with keys:
            count  - number of stadiums where it's a HR
            total  - total stadiums (30)
            parks  - list of dicts with:
                team, name, is_hr, fence_dist, fence_height, ball_height, margin_ft
    """
    # Convert Statcast spray angle → garesborn theta (0=LF pole, 90=RF pole)
    theta = 45.0 + spray_angle

    # Foul ball check
    if theta < 0 or theta > 90:
        return {
            "count": 0,
            "total": len(_STADIUMS),
            "parks": [],
            "error": "Foul ball — spray angle outside fair territory",
        }

    # Cache trajectories by elevation bucket (round to nearest 250 ft)
    # to avoid redundant simulations for similar elevations
    _traj_cache = {}

    results = []
    for team, info in _STADIUMS.items():
        fence_dist = _eval_fence_distance(info["dist"], theta)
        fence_ht = _eval_fence_height(info["height"], theta)

        # Round elevation to nearest 250 ft for caching
        elev_key = round(info["elevation"] / 250) * 250
        temp_key = round(info["temp"] / 5) * 5
        cache_key = (elev_key, temp_key)
        if cache_key not in _traj_cache:
            _traj_cache[cache_key] = _simulate_trajectory(
                exit_velo, launch_angle, elev_key, temp_key
            )
        traj = _traj_cache[cache_key]

        ball_ht = _ball_height_at_distance(traj, fence_dist)
        if ball_ht is not None:
            margin = ball_ht - fence_ht
            is_hr = margin > 0
        else:
            margin = -999
            is_hr = False

        results.append({
            "team": team,
            "name": info["name"],
            "is_hr": is_hr,
            "fence_dist": round(fence_dist, 1),
            "fence_height": round(fence_ht, 1),
            "ball_height": round(ball_ht, 1) if ball_ht is not None else None,
            "margin_ft": round(margin, 1),
        })

    # Sort: HRs first (most margin), then non-HRs (closest to clearing)
    results.sort(key=lambda r: -r["margin_ft"])

    hr_count = sum(1 for r in results if r["is_hr"])
    return {
        "count": hr_count,
        "total": len(_STADIUMS),
        "parks": results,
    }


def estimated_distance(
    exit_velo: float,
    launch_angle: float,
    elevation_ft: float = 0,
    temp_f: float = 72.0,
) -> float:
    """Return estimated total distance (ft) the ball would travel."""
    traj = _simulate_trajectory(exit_velo, launch_angle, elevation_ft, temp_f)
    if traj:
        return round(traj[-1][0], 1)
    return 0.0
