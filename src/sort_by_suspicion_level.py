from pathlib import Path

import pandas as pd
import datetime

# Constants
DISTANCE_THRESHOLD = 60  # Portland city blocks are about 60 m wide
SUSPICIOUSLY_TOO_FAR = 1000  # If the trip average deviation is above this amount, put in 'bad' folder

print("Computing suspicion levels! Began: " + datetime.datetime.now().strftime("%H:%M:%S"))
print("Loading data from files. Began: " + datetime.datetime.now().strftime("%H:%M:%S"))

# Load the data
path = Path().joinpath('..', 'out', 'deviations')
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
savable.to_csv(Path().joinpath('..', 'out', 'suspicion_level', 'bad',
                               'suspiciously_too_far.csv'), index=False)
good_trips = dist.query('distance < @SUSPICIOUSLY_TOO_FAR').reset_index()
crumbs = pd.merge_ordered(crumbs, good_trips, how='inner', on=['tripID'])
crumbs = crumbs.iloc[:, 0:-1]  # drop rightmost column, which is the mean()
crumbs = crumbs.rename(columns={"distance_x": "distance"})
del bad_trips, dist, savable, good_trips

# Populate the suspicion level field with the % of crumbs above DISTANCE_THRESHOLD
print("Calculating the % of deviations above threshold for recorded trips.")
num = crumbs.query('distance > @DISTANCE_THRESHOLD').groupby(['tripID']).count()[['distance']]
denom = crumbs.groupby(['tripID']).count()[['distance']]
new_val = num['distance'] / denom['distance']
crumbs = pd.merge_ordered(crumbs, new_val, how='inner', on=['tripID']).fillna(0)
crumbs = crumbs.rename(columns={"distance_y": "suspicionLevel"})
crumbs = crumbs.rename(columns={"distance_x": "distance"})
crumbs = crumbs.sort_values('suspicionLevel')
del num, denom, new_val

# Save the results
print("Saving results to csv...")
crumbs.to_csv(Path().joinpath('..', 'out', 'suspicion_level', 'suspiciously_too_far.csv'), index=False)
print("Operations complete!")
