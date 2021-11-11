##############################################################################

airport_icao = "ESSA"
#airport_icao = "ESGG"
#airport_icao = "EIDW" # Dublin
#airport_icao = "LOWW" # Vienna

year = '2019'

#months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
months = ['10']

##############################################################################

import os

DATA_DIR = os.path.join("data", airport_icao + '_rwy')
DATA_DIR = os.path.join(DATA_DIR, year)

INPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_50NM_" + year)
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_50NM_" + year)

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
import time
start_time = time.time()

def check_51NM_circle_contains_point(point):
    
    central_point = (central_lat, central_lon)
    distance = geodesic(central_point, point).meters
    
    if distance < 51*1852:
        return True
    else:
        return False

for month in months:
    print(month)
    
    number_of_weeks = (5, 4)[month == '02' and not calendar.isleap(int(year))]
    #number_of_weeks = 1
        
    for week in range(0, number_of_weeks):
        
        print(airport_icao, year, month, week+1)
        
        filename = 'osn_' + airport_icao + '_states_50NM_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        
        full_filename = os.path.join(INPUT_DIR, filename)
        
        
        df = pd.read_csv(full_filename, sep=' ',
                                 names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'beginDate', 'endDate'],
                                 dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'altitude':float, 'velocity':int, 'beginDate':str, 'endDate':str})

        df.set_index(['flightId', 'sequence'], inplace = True)

        flight_id_num = len(df.groupby(level='flightId'))
        count = 0

        for flight_id, flight_id_group in df.groupby(level='flightId'):
            
            count = count + 1
            print(airport_icao, year, month, week+1, flight_id_num, count, flight_id)
            
            ###################################################################
            # Latitude or longitude outside of 50NM too much (>51NM)
            ###################################################################
            
            
            flight_states_df = flight_id_group.copy() 
            
            flight_states_df.reset_index(drop = False, inplace = True)
            df_len = len(flight_states_df)
            flight_states_df.set_index('sequence', inplace=True)
            
            remove = 0
            if not flight_states_df.empty:
                
                for seq, row in flight_states_df.iterrows():
                    
                    point = (row["lat"], row["lon"])
                    
                    if not check_51NM_circle_contains_point(point):
                        remove = 1
                        break
                        
            if remove == 1:
                df = df.drop(flight_id)
                continue
        
        
        filename = 'osn_' + airport_icao + '_states_50NM_' + year + '_' + month + '_week' + str(week + 1) + '.csv'

        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=False, index=True)

print((time.time()-start_time)/60)
