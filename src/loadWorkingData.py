from pathlib import Path
import pandas as pd

new_path = Path().joinpath('.', 'out', 'suspicion_level', 'suspiciously_too_far.csv')
df = pd.read_csv(new_path)

trips = df.groupby('tripID').mean()
trips = trips.reset_index()
trips = trips[['tripID', 'suspicionLevel']]

trips = trips.sort_values('suspicionLevel')
trips = trips.query('suspicionLevel < 1 & suspicionLevel > 0')

