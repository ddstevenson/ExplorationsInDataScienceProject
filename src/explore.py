from pathlib import Path
import pandas as pd
import datetime
import matplotlib.pyplot as plt

path = Path().joinpath('OriginalData', 'cyclic_data_20200224_0320_wkd')
files = path.glob("*.tsv")
li = []

for filename in files:
    df = pd.read_csv(filename, sep='\t', header=0)
    li.append(df)

crumbs = pd.concat(li, axis=0, ignore_index=True)
cad_avl = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_CAD_AVL_trips_Feb+Mar2020', 'C-Tran_CAD_AVL_trips_Feb'
                                                                                          '+Mar2020.csv'))
# Set up joins
cad_avl = cad_avl[['trip_id', 'route_number']].drop_duplicates(['trip_id', 'route_number'])
crumbs.insert(0, 'TRIP_NO', crumbs['EVENT_NO_TRIP'])
cad_avl.insert(0, 'trip_no', cad_avl['trip_id'])
crumbs.set_index('TRIP_NO', inplace=True)
cad_avl.set_index('trip_no', inplace=True)
df = crumbs.join(cad_avl)

# 1. Which files contain the bread crumb data?
# a) how much data is there?
# b) Specifically, how many files? 4 files
# c) how many MB (unzipped)? 496 MB

# 2. How many bread crumb readings?
# a) Total for all days 8 623 470
crumbs_quan_readings = crumbs.count()

# b) For March 13, 2020 423 128
crumbs_quan_readings_filter = crumbs.query('OPD_DATE == "13-MAR-20"').count()

# 3. How many distinct vehicles are tracked?
# a) For all days 117
crumbs_dist_vehic = crumbs['VEHICLE_ID'].nunique()

# b) For March 9, 2020 103
crumbs_dist_vehic_filter = crumbs.query('OPD_DATE == "09-MAR-20"')['VEHICLE_ID'].nunique()

# 4. How many routes? 29 routes used over this period
crumbs_dist_trips = crumbs['EVENT_NO_TRIP'].nunique()

# 5. Which dates are tracked in our sample data? 60 unique dates ranging from 2/1 to 3/31
a = crumbs.max()
b = crumbs.min()

# 6. the ACT_TIME column seems to represent the time at which the bread crumb sensor reading was recorded.
# a) what are the units for this field? Int 64 in seconds past midnight
crumbs_datatypes = crumbs.dtypes
cad_avl_datatypes = cad_avl.dtypes

# b) Produce a copy of the breadcrumb data with the fields
# OPD_DATE and ACT_TIME reduced to a single field called TIMESTAMP
df2 = df.copy()
df2.insert(9, 'TIMESTAMP', float)
prog = 0
for index, row in df2.iterrows():
    ts = datetime.datetime
    ts = ts.strptime(row['OPD_DATE'], '%d-%b-%y') + datetime.timedelta(seconds=row['ACT_TIME'])
    df2.at[index, 'TIMESTAMP'] = ts.timestamp()
    if prog % 80000 == 0:  # About 8 mil records total
        print(str(prog) + ' records processed (of ' + str(df2['OPD_DATE'].size)
              + '): currently ' + str(df2.at[index, 'TIMESTAMP']))
    prog += 1

del df2['OPD_DATE']
del df2['ACT_TIME']
df2.to_csv(Path().joinpath('out', 'crumbs_TIMESTAMP.csv'))

# Does this conversion save space in the data # or does it use more space? Save space
# since 2x fields use more than 1.
mem1 = df2.memory_usage()
mem2 = df.memory_usage()

# 7. For the route with route_short_name=’78’:
# How many trips are executed for that route each day? 24
num_trips = df.query('route_number == 78').groupby(['trip_number', 'OPD_DATE']).count().reset_index()
# Is it the same number of trips each day? No - this route only appears to have run for four days

# Compute the time taken for each trip and summarize these times as follows:
trips = pd.DataFrame(df.query('route_number == 78')['EVENT_NO_TRIP'].unique())
trips.insert(1, "trip_duration", int)
trips.columns = ['trip_number', 'trip_duration']
prog = 0
for index, row in trips.iterrows():
    a_trip = df.query('EVENT_NO_TRIP == @row["trip_number"]')
    trips.at[index, 'trip_duration'] = a_trip['ACT_TIME'].max() - a_trip['ACT_TIME'].min()
    if prog % 100 == 0:  # about 2k rows to be processed total
        print(str(prog) + ' records processed (of ' + str(trips['trip_number'].size)
              + '): currently ' + str(trips.at[index, 'trip_duration']))
    prog += 1

# Minimum trip time? 116 seconds
min_trip_time = trips['trip_duration'].min()

# Maximum trip time? 8975 seconds
max_trip_time = trips['trip_duration'].max()

# Average trip time? 1826 seconds
avg_trip_time = trips['trip_duration'].mean()

# Standard Deviation of trip time? 1302 seconds
std_trip_time = trips['trip_duration'].std

# 8. Plot
# The probability distribution of trip times for the route with route_short_name=‘67’
trips = pd.DataFrame(df.query('route_number == 67')['EVENT_NO_TRIP'].unique())
trips.insert(1, "trip_duration", int)
trips.columns = ['trip_number', 'trip_duration']
prog = 0
for index, row in trips.iterrows():
    a_trip = df.query('EVENT_NO_TRIP == @row["trip_number"]')
    trips.at[index, 'trip_duration'] = a_trip['ACT_TIME'].max() - a_trip['ACT_TIME'].min()
    if prog % 100 == 0:  # about 2k rows to be processed total
        print(str(prog) + ' records processed (of ' + str(trips['trip_number'].size)
              + '): currently ' + str(trips.at[index, 'trip_duration']))
    prog += 1

trips['trip_duration'].plot.density()
plt.show()

# The cumulative distribution function of trip times for the same route.
# (assume: 'the same route' means same route as above, and not 'each route'.)
trips['trip_number'].hist(bins=50, histtype='step', cumulative=True)
plt.show()

# 9. GPS_LONGITUDE and GPS_LATITUDE
# Do these values correspond to locations within Clark County?
# What is the bounding box for the GPS locations? That is what are the minimum and
# maximum latitude and longitude coordinates? If you plot these on a map
# (using maps.google.com) does the bounding box roughly correspond with Clark county?
# HINT: take max and min x and y coords and make a box in google; you're done
x_max = df['GPS_LONGITUDE'].max()
x_min = df['GPS_LONGITUDE'].min()
y_max = df['GPS_LATITUDE'].max()
y_min = df['GPS_LATITUDE'].min()

# https://www.google.com/maps/d/u/0/edit?mid=1UqqMXSJlmm4CjUNfsFeDconL_vLoXqGa&ll=45.603710897494224%2C-122.61839829948197&z=10
