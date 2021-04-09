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
INPUT_DIR = os.path.join(DATA_DIR, "states_close_to_TMA_downloaded_" + year)
OUTPUT_DIR = os.path.join(DATA_DIR, "states_TMA_" + year)

input_filename = 'states_close_to_TMA_opensky_downloaded_' + year + '_' + month + '_' + day + '_' + str(hour_begin) + '_' + str(hour_end) + '.csv'
output_filename = "states_TMA_opensky_" + year + '_' + month + '_' + day + '_' + str(hour_begin) + '_' + str(hour_end) + '.csv'

full_input_filename = os.path.join(INPUT_DIR, input_filename)
full_output_filename = os.path.join(OUTPUT_DIR, output_filename)


def get_states_inside_TMA(states_df, csv_output_file):

    states_inside_TMA_df = pd.DataFrame()

    number_of_flights = len(states_df.groupby(level='flightId'))
    count = 1
    for flight_id, new_df in states_df.groupby(level='flightId'):
        print(number_of_flights, count)
        count = count + 1
        first_point_index = get_first_point_index(flight_id, new_df)
        
        if first_point_index==-1:
            continue
        
        new_df_inside_TMA = new_df.iloc[first_point_index:]
        #if new_df.iloc[entry_point_index-1]['date'] == new_df.iloc[-1]['date']:
        
        new_df_inside_TMA_length = len(new_df_inside_TMA)
        
        sequence_list = list(range(new_df_inside_TMA_length))
        
        new_df_inside_TMA = new_df_inside_TMA.sort_values(by=['timestamp'])
        
        new_df_inside_TMA['sequence'] = sequence_list
        
        states_inside_TMA_df = states_inside_TMA_df.append(new_df_inside_TMA)
        
    
    number_of_flights = len(states_inside_TMA_df.groupby(level='flightId'))
    
    print("States inside TMA number of flights", number_of_flights)

    states_inside_TMA_df.to_csv(csv_output_file, sep=' ', encoding='utf-8', float_format='%.6f', header=None)



def get_first_point_index(flight_id, new_df):
    
    lat = 0
    lon = 0
    for seq, row in new_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if (check_TMA_contains_point(Point(lon, lat))):
            print(seq)
            return seq
    
    print("-1", lat, lon)
    return -1

def check_TMA_contains_point(point):

    lons_lats_vect = np.column_stack((TMA_lon, TMA_lat)) # Reshape coordinates
    polygon = Polygon(lons_lats_vect) # create polygon

    return polygon.contains(point)


df = pd.read_csv(full_input_filename, sep=' ',
                    names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'endDate'],
                    index_col=[0,1],
                    dtype={'flightId':str, 'sequence':int, 'timestamp':str, 'lat':float, 'lon':float, 'altitude':float, 'velocity':float, 'endDate':str})
    
print("Number of flights: ")
print(len(df.groupby(level='flightId')))

get_states_inside_TMA(df, full_output_filename)
