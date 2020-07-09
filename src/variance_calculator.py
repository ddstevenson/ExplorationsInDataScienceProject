from sympy.geometry import Point
from pathlib import Path
import pandas as pd


def get_distance(x, y, x1, y1):
    return Point((x, y)).distance(Point((x1, y1)))


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
df.sort_values(['route_index', 'shape_index', 'shape_pt_sequence'])

# Clean up
del routes
del trips
del shapes

# Calculate distance
prog = 0
prev = 0
for i in range(1, len(df)):
    x = 0
    if df.iat[i, df.columns.get_loc('shape_pt_sequence')] != 0:
        x = get_distance(df.iat[i, df.columns.get_loc('shape_pt_lat')],
                         df.iat[i, df.columns.get_loc('shape_pt_lon')],
                         df.iat[i-1, df.columns.get_loc('shape_pt_lat')],
                         df.iat[i-1, df.columns.get_loc('shape_pt_lon')]) + prev
    df.iat[i, df.columns.get_loc('shape_dist_traveled')] = x
    prev = x
    if prog % 100 == 0:  # About 60k records total
        print(str(prog) + ' records processed (of ' + str(df['shape_pt_lon'].size)
              + '): currently ' + str(df.iat[i, df.columns.get_loc('shape_dist_traveled')]))
    prog += 1

df.to_csv(Path().joinpath('out', 'shapes_distance.csv'))
df = pd.read_csv(Path().joinpath('out', 'shapes_distance.csv'))


