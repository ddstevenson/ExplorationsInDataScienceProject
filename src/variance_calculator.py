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


# Extracts the xy coordinates from the gdf's geometry field
def extract_xy(df: gp.GeoDataFrame):
    return df.geometry.coords[0][0], df.geometry.coords[0][1]


# @Return False if SHAPE_DEVIATION_DIST[x] > SHAPE_DEVIATION_DIST[x + how_far] or
#   trip_id[x] == trip_id[x + how_far); True otherwise
def is_less_than_n_behind(df: gp.GeoDataFrame, how_far: int = 1) -> bool:
    dist_traveled = df['SHAPE_DEVIATION_DIST'].shift(UP * how_far, fill_value=0) - df['SHAPE_DEVIATION_DIST']
    is_diff_trip = (df['trip_id'] != df['trip_id'].shift(UP * how_far, fill_value=0))
    return ~((TOLERANCE < dist_traveled) | is_diff_trip)


# These functions assume that df['processing_distance'] is filled with the look-ahead distance
# @Return True if element belongs exactly n places ahead; false otherwise
def is_exactly_n_behind(df: gp.GeoDataFrame) -> bool:
    how_far = df['processing_distance'][0]
    return ~is_less_than_n_behind(df, how_far) & is_less_than_n_behind(df, how_far + 1)


# @Return True if element belongs exactly n places ahead AND has no n + 1 to check; false otherwise
def is_exactly_n_behind_and_terminal(df: gp.GeoDataFrame) -> bool:
    how_far = df['processing_distance'][0]
    is_terminal = (df['trip_id'] != df['trip_id'].shift(UP * how_far, fill_value=0))
    return is_exactly_n_behind(df) & is_terminal


def update_to_n_ahead(df: gp.GeoDataFrame) -> gp.GeoDataFrame:
    how_far = df['processing_distance'][0]
    return df.shift(UP * how_far)


def get_distance(row: gp.GeoDataFrame) -> float:
    return row.projected_point.distance(row.geometry)


def get_distance2(x1, y1, x2, y2):
    return numpy.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)


# compares geometry field to shape_line
def get_projection(row: gp.GeoDataFrame) -> (float, float):
    x = ops.nearest_points(row.geometry, row.shape_line)
    return x[1].coords[0][0], x[1].coords[0][1]


def get_route_distance(row: gp.GeoDataFrame) -> float:
    return row.shape_line.project(row.geometry)  # Confusingly, the lib's distance method is called "project()"


def get_interpolations(row: gp.GeoDataFrame):
    # Interpolate() gets the point at n distance along the calling shape
    return row.shape_line.interpolate(row['scalar'])


print("Computing route deviations! Began: " + datetime.datetime.now().strftime("%H:%M:%S"))

new_path = Path().joinpath('..', 'data', 'original', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105')
routes = pd.read_csv(new_path.joinpath('routes.txt'))
shapes = pd.read_csv(new_path.joinpath('newshapes.txt'))
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

# Now grab breadcrumb data
path = Path().joinpath('..', 'data', 'original', 'cyclic_data_20200224_0320_wkd')
files = path.glob("*.tsv")
li = []

for filename in files:
    li.append(pd.read_csv(filename, sep='\t', header=0))

crumbs = pd.concat(li, axis=0, ignore_index=True)
del li, filename, files, path

# Clean up data - note, about 550 trips in crumbs have no associated cad_avl data
crumbs.dropna(subset=['GPS_LONGITUDE', 'GPS_LATITUDE', 'EVENT_NO_TRIP'], inplace=True)

# Add shape id to crumbs data
tripToShape = pd.read_csv(Path().joinpath('..', 'data', 'modified', 'trip2shape.csv'))
crumbs.insert(0, 'trip_id', crumbs['EVENT_NO_TRIP'])
crumbs.set_index('EVENT_NO_TRIP', inplace=True)
tripToShape.set_index('tripID', inplace=True)
crumbs = crumbs.join(tripToShape, how='outer')
del tripToShape

# Add vehicle_number and route_id to crumbs
cad_avl = pd.read_csv(
    Path().joinpath('..', 'data', 'original', 'C-Tran_CAD_AVL_trips_Feb+Mar2020', 'C-Tran_CAD_AVL_trips_Feb'
                                                                                  '+Mar2020.csv'))
cad_avl = cad_avl[['vehicle_number', 'trip_id', 'route_number']]
cad_avl = cad_avl.drop_duplicates(['vehicle_number', 'trip_id', 'route_number'])
cad_avl.set_index('trip_id', inplace=True)
crumbs = crumbs.join(cad_avl)
del cad_avl

# Reduce dataset size for testing
# crumbs = crumbs.query('shapeID == 49')

# Convert coordinates based on constants above
crumbs.insert(len(crumbs.columns), 'lat', crumbs['GPS_LATITUDE'] * LAT_DIST, allow_duplicates=True)
crumbs.insert(len(crumbs.columns), 'lon', crumbs['GPS_LONGITUDE'] * LON_DIST, allow_duplicates=True)
shp.insert(len(shp.columns), 'lat', shp['shape_pt_lat'] * LAT_DIST, allow_duplicates=True)
shp.insert(len(shp.columns), 'lon', shp['shape_pt_lon'] * LON_DIST, allow_duplicates=True)

# Init the for loop
# Added this loop to keep pipeline unclogged at the adjustment calculations,
# as I was not getting good time efficiency in final for loop due to the innermost iterrows()
total_set = crumbs
shapes = shp['shape_index'].unique().tolist()
for shape in shapes:
    crumbs = total_set.query('shapeID == @shape')
    if len(crumbs) == 0 or os.isfile('/Projects/ExplorationsInDataScienceProject/out/shapes/' +
                                     'deviation_breadcrumbs' + str(shape) + '.csv'):
        continue
    else:
        print('***********************************')
        print('***********************************')
        print("Computing route deviations for shape: " + str(shape) + " Began: " + datetime.datetime.now().strftime(
            "%H:%M:%S"))

    # At this point, we have shp = the route shapes, and crumbs = the breadcrumb data
    # Now it's time to find the "naive" projections onto the shape
    # First, we'll create a breadcrumbs gdf w/ correct shape geometry assigned
    crumbs = crumbs.dropna(subset=['lon', 'lat'])
    crumbs.insert(len(crumbs.columns), 'key', range(len(crumbs)))
    print("Writing point objects to breadcrumbs...")
    crumbs = gp.GeoDataFrame(crumbs, geometry=gp.points_from_xy(crumbs.lon, crumbs.lat))
    crumbsLines = gp.GeoDataFrame(crumbs[['key', 'shapeID', 'geometry']])
    crumbs.set_index('key', inplace=True)
    crumbsLines.set_index('key', inplace=True)

    # Route shape objects
    print("Writing route shape objects to breadcrumbs...")
    for shape_id in shp.drop_duplicates('shape_index')['shape_index']:
        cur_shape = shp.query('shape_index == @shape_id')
        cur_ls = geo.LineString(list(cur_shape[['lon', 'lat']].to_records(index=False)))
        crumbsLines.geometry.loc[crumbsLines['shapeID'] == shape_id] = cur_ls

    crumbs.insert(len(crumbs.columns), 'SHAPE_GPS_LONGITUDE', 0, allow_duplicates=True)
    crumbs.insert(len(crumbs.columns), 'SHAPE_GPS_LATITUDE', 0, allow_duplicates=True)
    crumbs.insert(len(crumbs.columns), 'SHAPE_DEVIATION_DIST', 0, allow_duplicates=True)
    crumbs.insert(len(crumbs.columns), 'shape_line', crumbsLines['geometry'])
    del crumbsLines, cur_ls, cur_shape, shape_id

    # Now naive projections onto crumbs
    print(
        "Writing naive route projections onto breadcrumbs... Began: " + datetime.datetime.now().strftime(
            "%H:%M:%S"))
    projections = crumbs.apply(get_projection, axis=1)
    crumbs[['SHAPE_GPS_LONGITUDE', 'SHAPE_GPS_LATITUDE']] = pd.DataFrame(projections)[0].to_list()
    crumbs = gp.GeoDataFrame(crumbs, geometry=gp.points_from_xy(crumbs.SHAPE_GPS_LONGITUDE, crumbs.SHAPE_GPS_LATITUDE))
    del projections

    # Also we need the distance of each naive projection along the shape
    print(
        "Writing distance of naive projections along route onto breadcrumbs... Began: " + datetime.datetime.now().strftime(
            "%H:%M:%S"))
    projections = crumbs.apply(get_route_distance, axis=1)
    crumbs['SHAPE_DEVIATION_DIST'] = pd.DataFrame(projections)
    del projections

    # Final step is to find out-of-order crumbs by comparing each to its next neighbor
    # Notice we only have to find crumbs that are ahead, since exactly the same number of
    # crumbs will be behind as will be ahead.
    print(
        "Matching out-of-order projections with their probable locations... Began: " + datetime.datetime.now().strftime(
            "%H:%M:%S"))
    crumbs.insert(len(crumbs.columns), 'processing_distance', 0)
    crumbs.insert(len(crumbs.columns), 'scalar', 0)
    for n in range(1, MAX_LOOK_FORWARD):
        crumbs['processing_distance'] = n
        # Handle the edge case where an out-of-order point goes to the end of the recorded trip
        crumbs['geometry'] = crumbs.mask(is_exactly_n_behind_and_terminal, update_to_n_ahead)['geometry']
        # Now we've got to find the point on the route halfway between the correct crumbs
        updates = crumbs.where(is_exactly_n_behind).dropna()
        p1 = updates['SHAPE_DEVIATION_DIST'].shift(UP * n, fill_value=0)
        p2 = updates['SHAPE_DEVIATION_DIST'].shift(UP * (n + 1), fill_value=0)
        updates['scalar'] = (p1 + p2) / 2  # Distance traveled along route of the new median points
        updates['geometry'] = gp.GeoDataFrame(updates.apply(get_interpolations, axis=1))
        # Didn't want to have to manually loop through these, but luckily not too many...
        for index, row in updates.iterrows():
            crumbs.loc[index] = row

    xy_vals = crumbs.apply(extract_xy, axis=1)
    crumbs[['SHAPE_GPS_LONGITUDE', 'SHAPE_GPS_LATITUDE']] = pd.DataFrame(xy_vals)[0].to_list()
    crumbs.insert(len(crumbs.columns), 'projected_point', crumbs['geometry'])
    crumbs = gp.GeoDataFrame(crumbs, geometry=gp.points_from_xy(crumbs.GPS_LONGITUDE, crumbs.GPS_LATITUDE))
    # crumbs['SHAPE_DEVIATION_DIST'] = crumbs.apply(get_distance, axis=1)*1000
    crumbs['SHAPE_DEVIATION_DIST'] = get_distance2(crumbs['lon'], crumbs['lat'],
                                                   crumbs['SHAPE_GPS_LONGITUDE'], crumbs['SHAPE_GPS_LATITUDE'])
    print("Crumbs more than 5 meters off course: " +
          str(crumbs.query('SHAPE_DEVIATION_DIST > 5')['SHAPE_DEVIATION_DIST'].count()))
    del p1, p2, n, updates, xy_vals

    # Add datetime column to crumbs table
    # This code is slow, but I don't want to bother with changing it
    print("Encoding timestamps... ")
    crumbs.insert(len(crumbs.columns), 'timestamp', float)
    for index, row in crumbs.iterrows():
        ts = datetime.datetime
        ts = ts.strptime(row['OPD_DATE'], '%d-%b-%y') + datetime.timedelta(seconds=row['ACT_TIME'])
        crumbs.at[index, 'timestamp'] = ts.timestamp()

    del ts, index, row

    # Add crumbs column for route_index, which will be emitted at the end
    crumbs.set_index('shapeID', inplace=True)
    joiner = shp.drop_duplicates(['route_index', 'shape_index'])[['route_index', 'shape_index']]
    crumbs = crumbs.join(joiner)
    del joiner

    # Save calculated data to csv file
    # @SpecNotes Spec says ...
    # tripID - the ID of the recorded trip
    # timestamp - the moment at which the reading was taken
    # vehicleID - identifies of the vehicle
    # origLatitude - the original sensor latitude value
    # origLongitude - the original sensor longitude value
    # shapeID - the shape in shapes.txt that corresponds to tripID
    # routeID - the route in route.txt corresponding to tripID
    # plannedTripID - the planned trip corresponding to tripID
    #       (this value is not always correct and probably should not be used in your analysis)
    # correctedLatitude - the corrected vehicle position
    # correctedLongitude - the corrected vehicle position
    # distance - the Euclidean distance (in meters) from the original sensor reading and the corrected vehicle position
    # angle - this value is not implemented.
    print("Saving file to csv...")
    crumbs['SHAPE_GPS_LATITUDE'] = crumbs['SHAPE_GPS_LATITUDE'] / LAT_DIST
    crumbs['SHAPE_GPS_LONGITUDE'] = crumbs['SHAPE_GPS_LONGITUDE'] / LON_DIST
    crumbs.insert(len(crumbs.columns), 'angle', 0)
    crumbs = crumbs.rename(columns={"trip_id": "tripID",
                                    "VEHICLE_ID": "vehicleID",
                                    "GPS_LATITUDE": "origLatitude",
                                    "GPS_LONGITUDE": "origLongitude",
                                    "route_index": "routeID",
                                    "SHAPE_GPS_LATITUDE": "correctedLatitude",
                                    "SHAPE_GPS_LONGITUDE": "correctedLongitude",
                                    "route_id": "routeID",
                                    "shape_index": "shapeID",
                                    "SHAPE_DEVIATION_DIST": "distance"})
    crumbs[['tripID', 'timestamp', 'vehicleID',
            'origLatitude', 'origLongitude', 'shapeID',
            'routeID', 'plannedTripID', 'correctedLatitude',
            'correctedLongitude', 'distance', 'angle']].to_csv(
        Path().joinpath('..', 'out', 'shapes', 'deviation_breadcrumbs' + str(shape) + '.csv'), index=False)
    print("File successfully written!")

# Finish up
print("Consolidating output ... Began: " + datetime.datetime.now().strftime("%H:%M:%S"))
path = Path().joinpath('..', 'out', 'shapes')
files = path.glob("*.csv")
li = []

for filename in files:
    li.append(pd.read_csv(filename, header=0))

crumbs = pd.concat(li, axis=0, ignore_index=True)
del li, filename, files, path
crumbs.to_csv(Path().joinpath('..', 'out', 'deviation_breadcrumbs.csv'), index=False)
print("Deviation computations complete! Ended at: " + datetime.datetime.now().strftime("%H:%M:%S"))
