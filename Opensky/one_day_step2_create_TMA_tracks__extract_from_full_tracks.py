import pandas as pd
import numpy as np
import calendar

from constants import *
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

import os

year = '2018'

month = '05'
day = '16'

hour_begin = 5
hour_end = 6

DATA_DIR = os.path.join("data", "one_day")
INPUT_DIR = os.path.join(DATA_DIR, "tracks_opensky_" + year)
OUTPUT_DIR = os.path.join(DATA_DIR, "tracks_TMA_opensky_" + year)


def get_all_tracks(csv_input_file):

    df = pd.read_csv(os.path.join(INPUT_DIR, csv_input_file), sep=' ',
                    names = ['flightId', 'sequence', 'endDate', 'callsign', 'icao24', 'date', 'time', 'timestamp',
                             'lat', 'lon', 'baroAltitude'],
                    index_col=[0,1],
                    dtype={'flightId':str, 'sequence':int, 'time':str, 'endDate':str, 'date':str})
    
    return df


# start from the last waypoint outside TMA
def get_tracks_inside_TMA(tracks_df, csv_output_file):

    tracks_inside_TMA_df = pd.DataFrame()

    number_of_flights = len(tracks_df.groupby(level='flightId'))
    count = 1
    for flight_id, new_df in tracks_df.groupby(level='flightId'):
        print(number_of_flights, count)
        count = count + 1
        entry_point_index = get_entry_point_index(flight_id, new_df)
        
        if entry_point_index == -1:
            continue
        
        if entry_point_index != 0:
            entry_point_index = entry_point_index - 1
        
        new_df_inside_TMA = new_df.iloc[entry_point_index:]
        #if new_df.iloc[entry_point_index-1]['date'] == new_df.iloc[-1]['date']:
        
        # reassign sequence
        new_df_inside_TMA_length = len(new_df_inside_TMA)
        
        sequence_list = list(range(new_df_inside_TMA_length))
        
        new_df_inside_TMA['sequence'] = sequence_list
        
        tracks_inside_TMA_df = tracks_inside_TMA_df.append(new_df_inside_TMA)

    tracks_inside_TMA_df.to_csv(os.path.join(OUTPUT_DIR, csv_output_file), sep=' ', encoding='utf-8', float_format='%.6f', header=None)



def get_entry_point_index(flight_id, new_df):
    
    for seq, row in new_df.groupby(level='sequence'):
        if (check_TMA_contains_point(Point(row.loc[(flight_id, seq)]['lon'], row.loc[(flight_id, seq)]['lat']))):
            return seq
        
    return -1


def check_TMA_contains_point(point):

    lons_lats_vect = np.column_stack((TMA_lon, TMA_lat)) # Reshape coordinates
    polygon = Polygon(lons_lats_vect) # create polygon

    return polygon.contains(point)  



import time
start_time = time.time()

input_filename = 'tracks_opensky_' + year + '_' + month + '_' + day + '_' + str(hour_begin) + '_' + str(hour_end) + '.csv'

all_tracks_df = get_all_tracks(input_filename)

output_filename = 'tracks_TMA_opensky_' + year + '_' + month + '_' + day + '_' + str(hour_begin) + '_' + str(hour_end) + '.csv'

get_tracks_inside_TMA(all_tracks_df, output_filename)


print((time.time()-start_time)/60)
