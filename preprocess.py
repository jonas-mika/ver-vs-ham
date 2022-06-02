# preprocess
import os
import numpy as np
import pandas as pd

cols = {
    # resuls
    'raceId': 'Race ID',
    'grid': 'Grid Position',
    'position_x': 'Final Position',
    'time_x': 'Finish Time',
    'points_x': 'Points Gained',
    'laps': 'Laps',

    # races
    'circuitId': 'Circuit ID',
    'name_x': 'Race Name',
    'round': 'Round',
    'date': 'Date',
    'url_x': 'Circuit URL',

    # driver
    'driverId': 'Driver ID',
    'forename': 'Driver First Name',
    'surname': 'Driver Last Name',
    'code': 'Driver Code',
    'number_x': 'Driver Number',
    'nationality': 'Nationality',
    'url_y': 'Driver URL',

    # drivers standings
    'points_y': 'Total Points',
    'position_y': 'Position After Race',
    'wins': 'Total Wins',

    # circuit
    'name_y': 'Circuit Name',
    'location': 'Circuit Location',
    'country': 'Circuit Country',
    'lat': 'Circuit Latitude',
    'lng': 'Circuit Longitude'
    }

results = pd.read_csv("data/results.csv")
races = pd.read_csv("data/races.csv")
circuits = pd.read_csv("data/circuits.csv")
drivers = pd.read_csv("data/drivers.csv")
driver_standings = pd.read_csv("data/driver_standings.csv")
laps = pd.read_csv("data/lap_times.csv")

# joins
data = results\
        .merge(races, on="raceId")\
        .merge(circuits, on="circuitId")\
        .merge(drivers, on="driverId")\
        .merge(driver_standings, on=["driverId", "raceId"])\
        .query('year == 2021')\
        .query('code == "VER" or code == "HAM"')\
        .reset_index()

# select and rename cols
data = data[cols.keys()].rename(columns=cols)

# deal with nans
data['Final Position'] = data['Final Position'].mask(data['Final Position'] == '\\N', 20)

data['Finish Time'] = data['Finish Time'].mask(data['Finish Time'] == '\\N', -1)

# make lap times frame
lap_times = laps\
              .merge(drivers, on="driverId")\
              .merge(races, on="raceId")\
              .query('year == 2021')\
              .query('code == "VER" or code == "HAM"')\
              .reset_index()

cols = {
    # lap times
    'raceId': 'Race ID',
    'lap': 'Lap',
    'position': 'Position',
    'time_x': 'Lap Time',
    'milliseconds': 'Lap Time (ms)',

    # driver
    'driverId': 'Driver ID',
    'code': 'Driver Code',

    # race
    'circuitId': 'Circuit ID',
    'name': 'Race Name',
    }

lap_times = lap_times[cols].rename(columns=cols)

start_pos = data[['Race ID', 'Grid Position', 'Driver ID', 'Driver Code', 'Circuit ID', 'Race Name']]
start_pos.loc[:, 'Lap'] = 0
start_pos.loc[:, 'Lap Time'] = 0
start_pos.loc[:, 'Lap Time (ms)'] = 0
start_pos.loc[:, 'Index'] = list(range(2480, 2480+data.shape[0]))
start_pos = start_pos.rename(columns={'Grid Position': 'Position'})
start_pos = start_pos.set_index('Index')
start_pos = start_pos[lap_times.columns]
start_pos = start_pos.drop('Driver ID', axis=1)

lap_times = pd.concat([lap_times, start_pos])

# os.makedirs('data/processed') if not os.path.exists('data/processed') else None
data.to_csv('data/f1-2021.csv')
lap_times.to_csv('data/laps.csv')
