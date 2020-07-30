import pandas as pd
import numpy as np
import os
import datetime
import math
import argparse
import matplotlib.pyplot as plt
import sys, traceback

#global declarations
routes_df = pd.DataFrame()
shapes_df = pd.DataFrame()
trips_df = pd.DataFrame()
cadavl_df = pd.DataFrame()
trip2shape_df = pd.DataFrame()
breadcrumbfiles = ["bos_2020022428.tsv","bos_20200301620.tsv","bos_2020030206.tsv","bos_2020030913.tsv"]
bc_df = pd.DataFrame()
DEBUG = True
need2translate = False

gtfsdir = "google_transit_20200105"
cadavlfile = "C-Tran_CAD_AVL_trips_Feb+Mar2020.csv"
shapesfile = os.path.join(gtfsdir,"shapes.txt")
trip2shape_file = "foo.csv"

breadcrumbfiles = ["cases/breadcrumbs_small.tsv"]

def readData(fname, s=',', parsed=[], idf=False):
	if (DEBUG): print("reading file:", fname)
	df = pd.read_csv(fname, sep=s, parse_dates=parsed, infer_datetime_format=idf,
					 low_memory=False)
	return df

# find the distance score for the input shape (shapeID) and recorded trip (tlis)
# this is a placeholder until we have a better 
# trip deviation metric (the measurement of how much a recorded trip deviates from 
# its planned trip)
def distanceScore(shapeID, tlis):
	
	# find all of the coordinates for the input shape
	df = shapes_df.loc[shapes_df['shape_id'] == shapeID]

	# calculate Manhattan distance from first point in recorded trip to first point in shape
	first_trip_coord = tlis[0]
	first_trip_lat = first_trip_coord[0]
	first_trip_lon = first_trip_coord[1]

	first_shape_lat = df['shape_pt_lat'].iloc[0]
	first_shape_lon = df['shape_pt_lon'].iloc[0]

	# calculate the Manhattan distance between the first coords of trip and shape
	# it would be more accurate to compute the Euclidean distance
	# but for our purposes Manhattan distance is good enough
	first_dist = abs(first_trip_lat - first_shape_lat) + abs(first_trip_lon - first_shape_lon)

	# calculate distance from end point of recorded trip to end point of shape
	last_trip_coord = tlis[-1]
	last_trip_lat = last_trip_coord[0]
	last_trip_lon = last_trip_coord[1]
	last_shape_lat = df['shape_pt_lat'].iloc[-1]
	last_shape_lon = df['shape_pt_lon'].iloc[-1]

	# compute Manhattan distance for final coordinates of shape and trip
	last_dist = abs(last_trip_lat - last_shape_lat) + abs(last_trip_lon - last_shape_lon)

	dist = first_dist + last_dist 

	return dist

# of all the trips/shapes that match this trip, which one is best?
def pickShapeFromList(slis, tripID):
	nshape = len(slis)
	if (nshape < 1):
		print("ERROR: found no matching shape for trip:", tripID)
		exit()
	if (nshape == 1):
		return(slis[0])

	df = bc_df.loc[bc_df['EVENT_NO_TRIP'] == tripID]

	tlis = []
	for index, row in df.iterrows():
		lat = row['GPS_LATITUDE']
		lon = row['GPS_LONGITUDE']
		coord = [lat, lon]
		tlis.append(coord)

	# choose the shape with the smallest deviation score
	retval = min(slis, key=lambda sid:distanceScore(sid, tlis))
	return(retval)

# from the recorded trip ID get the route information
def trip2route(rtrip):
	df = cadavl_df.loc[cadavl_df['trip_number'] == rtrip]
	if (df.empty):
		print("WARNING: trip", rtrip, "does not appear in the CAD/AVL data", cadavlfile)
		return -1, -1, "ERROR: ROUTE NOT FOUND IN CADAVL DATA"

	# all cadavl records with this recorded_trip ID should have the same route_number
	# so we just pick the first one
	routeShortName = df.route_number.iloc[0]

	# from the short name we can find the route's internal ID and long name
	df = routes_df.loc[routes_df['route_short_name'] == routeShortName]
	routeID = df.route_id.iloc[0]
	routeLongName = df.route_long_name.iloc[0]

	return (routeID, routeShortName, routeLongName)

# from the input trip identifier compute the corresponding shapeID
def trip2shape(rtrip):
	# convert trip ID to route number
	routeID, routeShortName, routeLongName = trip2route(rtrip)
	if (routeID < 0): return -1, -1

	# find the planned trips for this route_id
	df = trips_df.loc[(trips_df['route_id'] == routeID)]

	# from the list of planned trips find the full list of possible shapes
	shapeids_df = df['shape_id']
	shape_ids = df['shape_id'].unique()
	if (len(shape_ids) > 1):
		if (DEBUG): print("\ttrip", rtrip, "corresponds to more than one shape", shape_ids)

	# choose the shape that most closely matches the recorded trip's geometry
	shapeID = pickShapeFromList(shape_ids, rtrip)

	# find the planned trip that uses this shape
	# this is a kludge. 
	# instead of this we should find the planned trip with the start time nearest to rtrip
	tdf = trips_df.loc[trips_df['shape_id'] == shapeID]
	ptlist = tdf['trip_id'].tolist()
	plannedTripID = ptlist[0]  # take the first one in the list

	if (DEBUG): print(f"\tshapeID {shapeID} plannedTripID {plannedTripID}")
	return shapeID, plannedTripID

def output_all_trip2shape():
	if (DEBUG): print(f"opened file {trip2shape_file}")
	fil = open(trip2shape_file,"w+")
	fil.write("tripID,shapeID,plannedTripID\n")

	trip_ids = bc_df['EVENT_NO_TRIP'].unique()
	for tid in trip_ids:
		if (DEBUG): print(f"Processing tripID {tid}")
		shape_id, ptrip = trip2shape(tid)
		if (shape_id >= 0):
			fil.write(f"{tid},{shape_id},{ptrip}\n")
	fil.close()
	if (DEBUG): print(f"finished writing mapping data to {trip2shape_file}")

# read in all of the data and compute teh shapeID if needed
def ingest_data():
	global routes_df, shapes_df, trips_df, cadavl_df, tts_df, bc_df, shapeID
	try:
		if (DEBUG): print("Ingesting the Data...")
 
		routes_df = readData(os.path.join(gtfsdir,"routes.txt"))
		trips_df = readData(os.path.join(gtfsdir,"trips.txt"))
		cadavl_df = readData(cadavlfile)
		shapes_df = readData(shapesfile)

		for fname in breadcrumbfiles:
			df = readData(fname, s='\t', parsed=[1], idf=True)
			bc_df = bc_df.append(df)

		if (DEBUG): print("data cleaning")
		routes_df['route_short_name'] = routes_df['route_short_name'].astype(int)
		trips_df['route_id'] = trips_df['route_id'].astype(int)
		bc_df.rename(columns={'OPD_DATE': 'date'}, inplace=True)
		bc_df["ACT_TIME"] = pd.to_timedelta(bc_df["ACT_TIME"], "s")
		bc_df['TIMESTAMP'] = bc_df['date'] + bc_df['ACT_TIME']

		if (DEBUG): print("finished ingesting data files")

	except Exception as e:
		print("ERROR while ingesting data")
		print(e)
		traceback.print_exc(file=sys.stdout)
		exit(-1)

ingest_data()
output_all_trip2shape()




