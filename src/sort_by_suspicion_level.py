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



