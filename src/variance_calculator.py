from pathlib import Path
import pandas as pd
import numpy.linalg as la
import numpy


def get_distance(x1, y1, x2, y2):
    return la.norm(numpy.array((x1, y1)) - numpy.array((x2, y2)))


def get_projection(x, y, x1, y1, x2, y2):
    v = numpy.array((x1, y1))
    w = numpy.array((x2, y2))
    p = numpy.array((x, y))
    len_squared = (x2 - x1)**2 + (y2 - y1)**2
    # No need to check for length of 0 b/c route shapes don't have dup points
    t = max(0, min(1, numpy.dot(p-v, w-v) / len_squared))
    t = v + t * (w - v)
    return t[0], t[1]


routes = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                     'routes.txt'))
shapes = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                     'shapes.txt'))
trips = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                    'trips.txt'))

# Explanation: indices get lost in the merge, so we need to dup them
routes.insert(0, 'route_index', routes['route_id'])
trips.insert(0, 'shape_index', trips['shape_id'])

# Clean up this field for upcoming merge with breadcrumb data
routes['route_short_name'].replace(to_replace='Vine', value=50, inplace=True)
routes['route_short_name'] = pd.to_numeric(routes['route_short_name'])

# Join
df = routes.set_index('route_id').join(trips.set_index('route_id'))
df.drop_duplicates(['route_index', 'shape_index'], inplace=True)
df = df.set_index('shape_id').join(shapes.set_index('shape_id'))
df = df[['route_index', 'shape_index', 'shape_pt_sequence', 'shape_pt_lat', 'shape_pt_lon', 'shape_dist_traveled',
         'route_short_name', 'route_long_name']]
df['route_short_name'].replace(to_replace='Vine', value=50, inplace=True)
df.sort_values(['route_index', 'shape_index', 'shape_pt_sequence'])
del routes, trips, shapes

# Calculate distance traveled along path for each segment
df['shape_dist_traveled'] = df['shape_dist_traveled'].mask(df['shape_pt_sequence'] != 0,
                                                           get_distance(df.shift(1)['shape_pt_lat'],
                                                                        df['shape_pt_lat'],
                                                                        df.shift(1)['shape_pt_lon'],
                                                                        df['shape_pt_lon']))

# Now convert this distance to a running total along each segment on the shape
for index, row in df[['route_index', 'shape_index']].drop_duplicates(['route_index', 'shape_index']).iterrows():
    df.loc[
        (df['route_index'] == row['route_index']) & (df['shape_index'] == row['shape_index']), 'shape_dist_traveled'] = \
        df.loc[(df['route_index'] == row['route_index']) & (
                df['shape_index'] == row['shape_index']), 'shape_dist_traveled'].cumsum()

del index, row

# Now grab breadcrumb data
path = Path().joinpath('OriginalData', 'cyclic_data_20200224_0320_wkd')
files = path.glob("*.tsv")
li = []

for filename in files:
    li.append(pd.read_csv(filename, sep='\t', header=0))

crumbs = pd.concat(li, axis=0, ignore_index=True)
cad_avl = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_CAD_AVL_trips_Feb+Mar2020', 'C-Tran_CAD_AVL_trips_Feb'
                                                                                          '+Mar2020.csv'))
del li, filename, files, path

# Merge breadcrumbs with cad_avl data -> links trip_id to route_number;
# Both are needed to link with shapes data
cad_avl = cad_avl[['trip_id', 'route_number']].drop_duplicates(['trip_id', 'route_number'])
crumbs.insert(0, 'TRIP_NO', crumbs['EVENT_NO_TRIP'])
cad_avl.insert(0, 'trip_no', cad_avl['trip_id'])
crumbs.set_index('TRIP_NO', inplace=True)
cad_avl.set_index('trip_no', inplace=True)
crumbs = crumbs.join(cad_avl)
del cad_avl

# Clean up data - note, about 550 trips in crumbs have no associated cad_avl data
crumbs.dropna(subset=['GPS_LONGITUDE', 'GPS_LATITUDE', 'trip_id'], inplace=True)

# At this point, we have df = the route shapes, and crumbs = the breadcrumb data
