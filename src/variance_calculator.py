from sympy.geometry import Point
from pathlib import Path
import pandas as pd
import numpy


def get_distance(x1, y1, x2, y2):
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)


routes = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                     'routes.txt'))
shapes = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                     'shapes.txt'))
trips = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                    'trips.txt'))

# Explanation: indices get lost in the merge, so we need to dup them
routes.insert(0, 'route_index', routes['route_id'])
trips.insert(0, 'shape_index', trips['shape_id'])

# Join
df = routes.set_index('route_id').join(trips.set_index('route_id'))
df.drop_duplicates(['route_index', 'shape_index'], inplace=True)
df = df.set_index('shape_id').join(shapes.set_index('shape_id'))
df = df[['route_index', 'shape_index', 'shape_pt_sequence', 'shape_pt_lat', 'shape_pt_lon', 'shape_dist_traveled',
         'route_short_name', 'route_long_name']]
df['route_short_name'].replace(to_replace='Vine', value=50, inplace=True)
df.sort_values(['route_index', 'shape_index', 'shape_pt_sequence'])

# Clean up
del routes
del trips
del shapes

# Calculate distance (if shapes_distance.csv does not exist)
df['shape_dist_traveled'] = df['shape_dist_traveled'].mask(df['shape_pt_sequence'] != 0,
                               numpy.sqrt((df.shift(1)['shape_pt_lat'] - df[
                                   'shape_pt_lat']) ** 2 + (df.shift(1)['shape_pt_lon'] -
                                                 df['shape_pt_lon']) ** 2) + df.shift(1)['shape_dist_traveled'])

df.to_csv(Path().joinpath('out', 'shapes_distance.csv'))

# Run these if shapes_distance.csv exists
df = pd.read_csv(Path().joinpath('out', 'shapes_distance.csv'))
del df['shape_index']
df['route_short_name'].replace(to_replace='Vine', value=50, inplace=True)  # already wrote file
df['route_short_name'] = pd.to_numeric(df['route_short_name'])

# Now grab breadcrumb data
path = Path().joinpath('OriginalData', 'cyclic_data_20200224_0320_wkd')
files = path.glob("*.tsv")
li = []

for filename in files:
    tmp = pd.read_csv(filename, sep='\t', header=0)
    li.append(tmp)

crumbs = pd.concat(li, axis=0, ignore_index=True)
cad_avl = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_CAD_AVL_trips_Feb+Mar2020', 'C-Tran_CAD_AVL_trips_Feb'
                                                                                          '+Mar2020.csv'))
# Set up joins
cad_avl = cad_avl[['trip_id', 'route_number']].drop_duplicates(['trip_id', 'route_number'])
crumbs.insert(0, 'TRIP_NO', crumbs['EVENT_NO_TRIP'])
cad_avl.insert(0, 'trip_no', cad_avl['trip_id'])
crumbs.set_index('TRIP_NO', inplace=True)
cad_avl.set_index('trip_no', inplace=True)
crumbs = crumbs.join(cad_avl)
del cad_avl
