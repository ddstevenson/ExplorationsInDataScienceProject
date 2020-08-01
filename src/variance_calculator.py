from pathlib import Path

import geopandas as gp
import numpy
import pandas as pd
import shapely.geometry as geo
import shapely.ops as ops


def get_distance(x1, y1, x2, y2):
    return numpy.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)


def get_projection(row: gp.GeoDataFrame) -> (float, float):
    x = ops.nearest_points(row.geometry, row.shape_line)
    return x[0].coords[0][0], x[0].coords[0][1]


def get_route_distance(row: gp.GeoDataFrame) -> float:
    return row.shape_line.project(row.geometry)     # Confusingly, the lib's distance method is called "project()"


new_path = Path().joinpath('data', 'original', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105')
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
del routes, shapes, new_path, trips

# Calculate distance traveled along path for each segment
shp['shape_dist_traveled'].mask(shp['shape_pt_sequence'] != 0, get_distance(shp.shift(1)['shape_pt_lat'],
                                                                          shp.shift(1)['shape_pt_lon'],
                                                                          shp['shape_pt_lat'],
                                                                          shp['shape_pt_lon']), inplace=True)

# Now convert this distance to a running total along each segment on the shape
for index, row in shp[['route_index', 'shape_index']].drop_duplicates(['route_index', 'shape_index']).iterrows():
    shp.loc[(shp['route_index'] == row['route_index']) & (
            shp['shape_index'] == row['shape_index']), 'shape_dist_traveled'] = \
        shp.loc[(shp['route_index'] == row['route_index']) & (
                shp['shape_index'] == row['shape_index']), 'shape_dist_traveled'].cumsum()

del index, row

# Now grab breadcrumb data
path = Path().joinpath('data', 'original', 'cyclic_data_20200224_0320_wkd')
files = path.glob("*.tsv")
li = []

for filename in files:
    li.append(pd.read_csv(filename, sep='\t', header=0))

crumbs = pd.concat(li, axis=0, ignore_index=True)
del li, filename, files, path

# Clean up data - note, about 550 trips in crumbs have no associated cad_avl data
crumbs.dropna(subset=['GPS_LONGITUDE', 'GPS_LATITUDE', 'EVENT_NO_TRIP'], inplace=True)

# Add shape id to crumbs data
tripToShape = pd.read_csv(Path().joinpath('data', 'modified', 'trip2shape.csv'))
crumbs.insert(0, 'trip_id', crumbs['EVENT_NO_TRIP'])
crumbs.set_index('EVENT_NO_TRIP', inplace=True)
tripToShape.set_index('tripID', inplace=True)
crumbs = crumbs.join(tripToShape, how='outer')
del tripToShape

# Add vehicle_number and route_id to crumbs
cad_avl = pd.read_csv(
    Path().joinpath('data', 'original', 'C-Tran_CAD_AVL_trips_Feb+Mar2020', 'C-Tran_CAD_AVL_trips_Feb'
                                                                            '+Mar2020.csv'))
cad_avl = cad_avl[['vehicle_number', 'trip_id', 'route_number']]
cad_avl = cad_avl.drop_duplicates(['vehicle_number', 'trip_id', 'route_number'])
cad_avl.set_index('trip_id', inplace=True)
crumbs = crumbs.join(cad_avl)
del cad_avl

# Reduce dataset size for testing
crumbs = crumbs.query('shapeID == 49')

# At this point, we have shp = the route shapes, and crumbs = the breadcrumb data
# Now it's time to find the "naive" projections onto the shape
# First, we'll create a breadcrumbs gdf w/ correct shape geometry assigned
crumbs.dropna(subset=['GPS_LONGITUDE', 'GPS_LATITUDE'], inplace=True)
crumbs.insert(11, 'key', range(len(crumbs)))
print("Writing point objects to breadcrumbs...")
crumbs = gp.GeoDataFrame(crumbs, geometry=gp.points_from_xy(crumbs.GPS_LONGITUDE, crumbs.GPS_LATITUDE))
crumbsLines = gp.GeoDataFrame(crumbs[['key', 'shapeID', 'geometry']])
crumbs.set_index('key', inplace=True)
crumbsLines.set_index('key', inplace=True)

# Route shape objects
print("Writing route shape objects to breadcrumbs...")
for shape_id in shp.drop_duplicates('shape_index')['shape_index']:
    cur_shape = shp.query('shape_index == @shape_id')
    cur_ls = geo.LineString(list(cur_shape[['shape_pt_lon', 'shape_pt_lat']].to_records(index=False)))
    crumbsLines.geometry.loc[crumbsLines['shapeID'] == shape_id] = cur_ls
    print(shape_id)

crumbs.insert(12, 'SHAPE_GPS_LONGITUDE', 0, allow_duplicates=True)
crumbs.insert(13, 'SHAPE_GPS_LATITUDE', 0, allow_duplicates=True)
crumbs.insert(14, 'SHAPE_DEVIATION_DIST', 0, allow_duplicates=True)
crumbs.insert(15, 'shape_line', crumbsLines['geometry'])
del crumbsLines, cur_ls, cur_shape, shape_id

# Now naive projections (computationally intensive!)
print("Writing naive route projections onto breadcrumbs...")
projections = crumbs.apply(get_projection, axis=1)
crumbs[['SHAPE_GPS_LONGITUDE', 'SHAPE_GPS_LATITUDE']] = pd.DataFrame(projections)[0].to_list()
crumbs = gp.GeoDataFrame(crumbs, geometry=gp.points_from_xy(crumbs.SHAPE_GPS_LONGITUDE, crumbs.SHAPE_GPS_LATITUDE))
del projections

# Also we need the distance of each naive projection along the shape
projections = crumbs.apply(get_route_distance, axis=1)
crumbs['SHAPE_DEVIATION_DIST'] = pd.DataFrame(projections)
del projections

# Final step is to









