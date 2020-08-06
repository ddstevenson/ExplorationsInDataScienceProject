from pathlib import Path

import os.path as os
import geopandas as gp
import pandas as pd
import shapely.geometry as geo
import shapely.ops as ops
import datetime
import numpy

# Constants
DISTANCE_THRESHOLD = 60  # Portland city blocks are about 60 m wide
MINIMUM_STRAIGHT = 5  # Smallest sized straight this algorithm will analyze
SKIPS_PER_STRAIGHT = 3  # Number of below-threshold points we'll ignore
SUSPICIOUSLY_TOO_FAR = 1000  # Return None when route average distance is above this value


# true if the next MINIMUM_STRAIGHT crumb records are above DISTANCE_THRESHOLD; false otherwise
def has_minimum_straight(row: gp.DataFrame) -> bool:
    return True


from pathlib import Path

import os.path as os
import geopandas as gp
import pandas as pd
import shapely.geometry as geo
import shapely.ops as ops
import datetime
import numpy

# constants
UP = -1
DOWN = 1
TOLERANCE = -0.0000001  # Floats within this amount of one another are considered equal
MAX_LOOK_FORWARD = 30  # This number should be more than the length of longest contiguous seq. of out of order crumbs
LAT_DIST = 111.2 * 1000  # m per degree latitude at 45.63 degrees latitude (approximate)
LON_DIST = 77.76 * 1000  # m per degree longitude at 45.63 degrees latitude (approximate)


# @Return False if SHAPE_DEVIATION_DIST[x] < SHAPE_DEVIATION_DIST[x + how_far] or
#   trip_id[x] == trip_id[x + how_far); True otherwise
def is_less_than_n_ahead(df: gp.GeoDataFrame, how_far: int = 1) -> bool:
    dist_traveled = df['SHAPE_DEVIATION_DIST'] - df['SHAPE_DEVIATION_DIST'].shift(DOWN * how_far, fill_value=0)
    is_diff_trip = (df['trip_id'] != df['trip_id'].shift(DOWN * how_far, fill_value=0))
    return ~((TOLERANCE < dist_traveled) | is_diff_trip)


# These functions assume that df['processing_distance'] is filled with the look-ahead distance
# @Return True if element belongs exactly n places ahead; false otherwise
def is_exactly_n_ahead(df: gp.GeoDataFrame) -> bool:
    how_far = df['processing_distance'][0]
    return ~is_less_than_n_ahead(df, how_far) & is_less_than_n_ahead(df, how_far + 1)


# @Return True if element belongs exactly n places ahead AND has no n + 1 to check; false otherwise
def is_exactly_n_behind_and_terminal(df: gp.GeoDataFrame) -> bool:
    how_far = df['processing_distance'][0]
    is_terminal = (df['trip_id'] != df['trip_id'].shift(UP * how_far, fill_value=0))
    return is_exactly_n_behind(df) & is_terminal


def update_to_n_ahead(df: gp.GeoDataFrame) -> gp.GeoDataFrame:
    how_far = df['processing_distance'][0]
    return df.shift(UP * how_far)