from pathlib import Path

import os.path as os
import pandas as pd
import datetime
import numpy

# Constants
DISTANCE_THRESHOLD = 60  # Portland city blocks are about 60 m wide
MINIMUM_STRAIGHT = 5  # Smallest sized straight this algorithm will analyze
SKIPS_PER_STRAIGHT = 3  # Number of below-threshold points we'll ignore
SUSPICIOUSLY_TOO_FAR = 1000  # If the trip average deviation is above this amount, put in 'bad' folder


# true if the next MINIMUM_STRAIGHT crumb records are above DISTANCE_THRESHOLD; false otherwise
def has_minimum_straight(row: pd.DataFrame) -> bool:
    return True


print("Computing suspicion levels! Began: " + datetime.datetime.now().strftime("%H:%M:%S"))
print("Loading data from files. Began: " + datetime.datetime.now().strftime("%H:%M:%S"))

# Load the data
path = Path().joinpath('.', 'out', 'deviations')
files = path.glob("*.csv")
li = []

for filename in files:
    li.append(pd.read_csv(filename, header=0))

crumbs = pd.concat(li, axis=0, ignore_index=True)
del li, filename, files, path

# Quick cleanup - remove this if your dataset doesn't have an unwanted two columns on left side
crumbs = crumbs.iloc[:, 2:]

# First filter out trips above the SUSPICIOUSLY_TOO_FAR threshold
print("Filtering out trips that are suspiciously too far from assigned routes. Began: " +
      datetime.datetime.now().strftime("%H:%M:%S"))
dist = crumbs.groupby(['tripID']).mean()[['distance']]
bad_trips = dist.query('distance >= @SUSPICIOUSLY_TOO_FAR').reset_index()
savable = pd.merge_ordered(crumbs, bad_trips, how='inner', on=['tripID'])  # select ... where ... in
savable = savable.iloc[:, 0:-1]  # drop rightmost column, which is the mean()
savable = savable.rename(columns={"distance_x": "distance"})
savable.to_csv(Path().joinpath('.', 'out', 'suspicion_level', 'bad',
                               'suspiciously_too_far.csv'), index=False)
good_trips = dist.query('distance < @SUSPICIOUSLY_TOO_FAR').reset_index()
crumbs = pd.merge_ordered(crumbs, good_trips, how='inner', on=['tripID'])
crumbs = crumbs.iloc[:, 0:-1]  # drop rightmost column, which is the mean()
crumbs = crumbs.rename(columns={"distance_x": "distance"})
del bad_trips, dist, savable, good_trips

# Iterate through shapes to form a pipeline
crumbs.insert(len(crumbs.columns), 'suspicionLevel', 0)
shapes = crumbs['shapeID'].unique().tolist()
for shape in shapes:
    crumb = crumbs.query('shapeID == @shape')
    if len(crumb) == 0 or os.isfile('/Projects/ExplorationsInDataScienceProject/out/suspicion_level/' +
                                     'suspicion_ranked_' + str(shape) + '.csv'):
        continue
    else:
        print('***********************************')
        print("Computing suspicion levels for shape: " + str(shape) + " Began: " + datetime.datetime.now().strftime(
            "%H:%M:%S"))

    trips = crumb['tripID'].unique().tolist()
    for tripID in trips:
        trip = crumb.query('tripID == @tripID')
        good

    # Save off this chunk of shapes to a file and start a new shape
    print("Saving file to csv...")
    crumb[['tripID', 'timestamp', 'vehicleID',
            'origLatitude', 'origLongitude', 'shapeID',
            'routeID', 'plannedTripID', 'correctedLatitude',
            'correctedLongitude', 'distance', 'angle', 'suspicionLevel']].to_csv(
        Path().joinpath('..', 'out', 'shapes', 'suspicion_ranked_' + str(shape) + '.csv'), index=False)
    print("File successfully written!")

# Finish up
print("Consolidating output ... Began: " + datetime.datetime.now().strftime("%H:%M:%S"))
path = Path().joinpath('..', 'out', 'suspicion_level')
files = path.glob("*.csv")
li = []

for filename in files:
    li.append(pd.read_csv(filename, header=0))

crumbs = pd.concat(li, axis=0, ignore_index=True)
del li, filename, files, path
crumbs.to_csv(Path().joinpath('..', 'out', 'final', 'suspicion_ranked.csv'), index=False)
print("Suspicion level computations complete! Ended at: " + datetime.datetime.now().strftime("%H:%M:%S"))