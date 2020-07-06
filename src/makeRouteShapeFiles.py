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
crumbs.set_index('EVENT_NO_TRIP')
cad_avl.set_index('trip_number')
df = crumbs.join(cad_avl[['trip_number', 'route_number']])