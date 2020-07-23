from pathlib import Path
import pandas as pd
import numpy


def get_distance(x1, y1, x2, y2):
    return numpy.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)


def get_projection(x, y, x1, y1, x2, y2):
    v = numpy.array((x1, y1))
    w = numpy.array((x2, y2))
    p = numpy.array((x, y))
    len_squared = (x2 - x1) ** 2 + (y2 - y1) ** 2
    # No need to check for length of 0 b/c route shapes don't have dup points
    t = max(0, min(1, numpy.dot(p - v, w - v) / len_squared))
    t = v + t * (w - v)
    return t[0], t[1]


new_path = Path().joinpath("..", 'OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105')
routes = pd.read_csv(new_path.joinpath('routes.txt'))
shapes = pd.read_csv(new_path.joinpath('shapes.txt'))
trips = pd.read_csv(new_path.joinpath('trips.txt'))

# Explanation: indices get lost in the merge, so we need to dup them
routes.insert(0, 'route_index', routes['route_id'])
trips.insert(0, 'shape_index', trips['shape_id'])

# Clean up this field for upcoming merge with breadcrumb data
routes['route_short_name'].replace(to_replace='Vine', value=50, inplace=True)
routes['route_short_name'] = pd.to_numeric(routes['route_short_name'])

# Join routes to trips to shapes
shp = routes.set_index('route_id').join(trips.set_index('route_id'))
shp.drop_duplicates(['route_index', 'shape_index'], inplace=True)
shp = shp.set_index('shape_id').join(shapes.set_index('shape_id'))
shp = shp[['route_index', 'shape_index', 'shape_pt_sequence', 'shape_pt_lat', 'shape_pt_lon', 'shape_dist_traveled',
           'route_short_name', 'route_long_name']]
shp['route_short_name'].replace(to_replace='Vine', value=50, inplace=True)
shp.sort_values(['route_index', 'shape_index', 'shape_pt_sequence'])
del routes, trips, shapes, new_path

# Calculate distance traveled along path for each segment
shp['shape_dist_traveled'] = shp['shape_dist_traveled'].mask(shp['shape_pt_sequence'] != 0,
                                                             get_distance(shp.shift(1)['shape_pt_lat'],
                                                                          shp.shift(1)['shape_pt_lon'],
                                                                          shp['shape_pt_lat'],
                                                                          shp['shape_pt_lon']))

# Now convert this distance to a running total along each segment on the shape
for index, row in shp[['route_index', 'shape_index']].drop_duplicates(['route_index', 'shape_index']).iterrows():
    shp.loc[(shp['route_index'] == row['route_index']) & (
            shp['shape_index'] == row['shape_index']), 'shape_dist_traveled'] = \
        shp.loc[(shp['route_index'] == row['route_index']) & (
                shp['shape_index'] == row['shape_index']), 'shape_dist_traveled'].cumsum()

del index, row

# Now grab breadcrumb data
path = Path().joinpath("..", 'OriginalData', 'cyclic_data_20200224_0320_wkd')
files = path.glob("*.tsv")
li = []

for filename in files:
    li.append(pd.read_csv(filename, sep='\t', header=0))

crumbs = pd.concat(li, axis=0, ignore_index=True)
cad_avl = pd.read_csv(
    Path().joinpath("..", 'OriginalData', 'C-Tran_CAD_AVL_trips_Feb+Mar2020', 'C-Tran_CAD_AVL_trips_Feb'
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

# At this point, we have shp = the route shapes, and crumbs = the breadcrumb data
# Now it's time to find the "naive" projections onto the shape
crumbs.insert(8, 'SHAPE_GPS_LONGITUDE', 0, allow_duplicates=True)
crumbs.insert(9, 'SHAPE_GPS_LATITUDE', 0, allow_duplicates=True)
crumbs.insert(10, 'SHAPE_DIST_TRAVELED', 0, allow_duplicates=True)
for shape_id in shp.drop_duplicates('shape_index')['shape_index']:
    print(shape_id)
    # TODO: Build the shape coordinate arrays
    # TODO: Assemble the crumbs matching this shape
