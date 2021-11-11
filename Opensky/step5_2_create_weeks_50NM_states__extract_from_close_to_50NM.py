##############################################################################

airport_icao = "ESSA"
#airport_icao = "ESGG"
#airport_icao = "EIDW" # Dublin
#airport_icao = "LOWW" # Vienna

departure = False

year = '2019'

#months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
months = ['10']

##############################################################################

import os

DATA_DIR = os.path.join("data", airport_icao + '_rwy')
DATA_DIR = os.path.join(DATA_DIR, year)
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_close_to_50NM_fixed_lat_lon_" + year)
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_50NM_extracted_" + year)

if not os.path.exists(INPUT_DIR):
    os.makedirs(INPUT_DIR)
    
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


import pandas as pd
import numpy as np
import calendar

from geopy.distance import geodesic

if airport_icao == "ESSA":
    from constants_ESSA import *
elif airport_icao == "ESGG":
    from constants_ESGG import *
elif airport_icao == "EIDW":
    from constants_EIDW import *
elif airport_icao == "LOWW":
    from constants_LOWW import *

def get_states_inside_50NM(states_df, month, week):
    
    filename = airport_icao + '_states_50NM_extracted_' + year + '_' + month + '_week' + str(week) + '.csv'
    if departure:
        filename = 'osn_departure_' + filename
    else:
        filename = 'osn_' + filename
     
    full_output_filename = os.path.join(OUTPUT_DIR, filename)

    states_inside_50NM_df = pd.DataFrame()

    number_of_flights = len(states_df.groupby(level='flightId'))
    count = 1
    for flight_id, flight_df in states_df.groupby(level='flightId'):
        print(airport_icao, year, month, week, number_of_flights, count)
        count = count + 1
        
        if departure:
            last_point_index = get_last_point_index(flight_id, flight_df)
            
            if last_point_index==-1:
                continue
            
            new_df_inside_50NM = flight_df.loc[flight_df.index.get_level_values('sequence') <= last_point_index]
            
        else: # arrival
            first_point_index = get_first_point_index(flight_id, flight_df)
        
            if first_point_index==-1:
                continue
        
            new_df_inside_50NM = flight_df.loc[flight_df.index.get_level_values('sequence') >= first_point_index]
        
            new_df_inside_50NM.reset_index(drop=False, inplace=True)
            new_df_inside_50NM.set_index(['flightId'], inplace=True)
        
            # reassign sequence
            new_df_inside_50NM_length = len(new_df_inside_50NM)
        
            sequence_list = list(range(new_df_inside_50NM_length))
        
            new_df_inside_50NM = new_df_inside_50NM.sort_values(by=['timestamp'])
        
            new_df_inside_50NM.drop(['sequence'], axis=1, inplace=True)
        
            new_df_inside_50NM['sequence'] = sequence_list
        
        new_df_inside_50NM = new_df_inside_50NM[['sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'beginDate', 'endDate']]

        states_inside_50NM_df = states_inside_50NM_df.append(new_df_inside_50NM)
        
    
    number_of_flights = len(states_inside_50NM_df.groupby(level='flightId'))
    
    states_inside_50NM_df.to_csv(full_output_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=None, index=True)


def get_last_point_index(flight_id, flight_df):
    
    lat = 0
    lon = 0
    for seq, row in flight_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if (not check_50NM_circle_contains_point(Point(lon, lat))):
            #print(seq)
            #print(lon, lat)
            return seq-1
    
    print("-1", lat, lon)
    return -1

def get_first_point_index(flight_id, flight_df):
    
    lat = 0
    lon = 0
    for seq, row in flight_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if (check_50NM_circle_contains_point((lat, lon))):
            #print(seq)
            #print(lon, lat)
            return seq
    
    print("-1", lat, lon)
    return -1


def check_50NM_circle_contains_point(point):
    central_point = (central_lat, central_lon)
    distance = geodesic(central_point, point).meters
    
    if distance < 50*1852:
        return True
    else:
        return False



import time
start_time = time.time()


for month in months:
    
    number_of_weeks = (5, 4)[month == '02' and not calendar.isleap(int(year))]
        
    #for week in range(0, number_of_weeks):
    for week in range(0, 1):
        
        #opensky states close to 50NM csv
        filename = airport_icao + '_states_close_to_50NM_fixed_lat_lon_' + year + '_' + month + '_week' + str(week) + '.csv'
        if departure:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_' + filename
        
        full_input_filename = os.path.join(INPUT_DIR, filename)
        
        df = pd.read_csv(full_input_filename, sep=' ',
            names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'beginDate', 'endDate'],
            index_col=[0,1],
            dtype={'flightId':str, 'sequence':int, 'timestamp':str, 'lat':float, 'lon':float, 'altitude':int, 'velocity':int, 'beginDate':str, 'endDate':str})

        get_states_inside_50NM(df, month, week + 1)

print((time.time()-start_time)/60)

