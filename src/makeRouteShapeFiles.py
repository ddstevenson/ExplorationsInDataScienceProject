from pathlib import Path
import pandas as pd
import shutil as st

routes = pd.read_csv(Path().joinpath('data','original', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                     'routes.txt'))
shapes = pd.read_csv(Path().joinpath('data','original', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                     'shapes.txt'))
trips = pd.read_csv(Path().joinpath('data','original', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                    'trips.txt'))

# Explanation: indices get lost in the merge, so we need to dup them
routes.insert(0, 'route_index', routes['route_id'])
trips.insert(0, 'shape_index', trips['shape_id'])

# Join
df = routes.set_index('route_id').join(trips.set_index('route_id'))
df.drop_duplicates(['route_index', 'shape_index'], inplace=True)
df = df.set_index('shape_id').join(shapes.set_index('shape_id'))
df = df[['route_index', 'shape_index', 'shape_pt_sequence', 'shape_pt_lat', 'shape_pt_lon', 'route_long_name']]

# Write each shape to its own file under a directory named the value of route_index
for subdir in Path().joinpath('out', 'shapes').iterdir():
    st.rmtree(subdir)

for route_id in df['route_index'].unique():
    Path().joinpath('out', 'shapes', str(route_id)).mkdir(exist_ok=True)
    subset = df.query('route_index == @route_id')
    for shape_id in subset['shape_index'].unique():
        df2 = subset.query('shape_index == @shape_id').sort_values(by='shape_pt_sequence')
        df2.to_csv(Path().joinpath('out', 'shapes', str(route_id), str(shape_id) + '.csv'))

