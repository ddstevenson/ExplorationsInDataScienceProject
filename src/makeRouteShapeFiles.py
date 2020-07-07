from pathlib import Path
import pandas as pd

routes = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                     'routes.txt'))
shapes = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                     'shapes.txt'))
trips = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                    'trips.txt'))

# Explanation: indices get lost in the merge, so we need to dup them
routes.insert(0, 'route_index', routes['route_id'])
trips.insert(0, 'shape_index', trips['shape_id'])

# Join
df = routes.set_index('route_id').join(trips.set_index('route_id'))
df.drop_duplicates(['route_index', 'shape_index'], inplace=True)
df = df.set_index('shape_id').join(shapes.set_index('shape_id'))
df = df[['route_index', 'shape_index', 'shape_pt_sequence', 'shape_pt_lat', 'shape_pt_lon']]

# Write each route out to its own file df.sort_values(by=['route_index', 'shape_pt_sequence'], inplace=True)
vals = df['route_index'].unique()
for val in vals:
    df2 = df.query('route_index == @val & shape_pt_sequence == null')
    df2 = df2.sort_values(by='shape_pt_sequence')
    df2.to_csv(Path().joinpath('out', 'shapes', str(val) + '.csv'))
