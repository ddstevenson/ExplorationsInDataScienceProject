import sympy as smp
from pathlib import Path
import pandas as pd
import src.variance_calculator_lib as defs

routes = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                     'routes.txt'))
shapes = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                     'shapes.txt'))
trips = pd.read_csv(Path().joinpath('OriginalData', 'C-Tran_GTFSfiles_20200105', 'google_transit_20200105',
                                    'trips.txt'))

shapes = defs.prepare_shapes(routes, shapes, trips)
del routes
del trips
