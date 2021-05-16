##############################################################################

#airport_icao = "ESSA"
#airport_icao = "ESGG"
airport_icao = "EIDW" # Dublin
#airport_icao = "LOWW" # Vienna

year = '2019'

#months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
months = ['10']

##############################################################################

import os

DATA_DIR = os.path.join("data", airport_icao)
DATA_DIR = os.path.join(DATA_DIR, year)

INPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_TMA_" + year)
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_TMA_" + year)

import pandas as pd
import numpy as np
import calendar

import time
start_time = time.time()


for month in months:
    print(month)
    
    number_of_weeks = (5, 4)[month == '02' and not calendar.isleap(int(year))]
    #number_of_weeks = 1
        
    for week in range(0, number_of_weeks):
        
        print(airport_icao, year, month, week+1)
        
        filename = 'osn_' + airport_icao + '_states_TMA_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        
        full_filename = os.path.join(INPUT_DIR, filename)
        
        
        df = pd.read_csv(full_filename, sep=' ',
                                 names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'endDate'],
                                 dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'altitude':float, 'endDate':str})

        df.set_index(['flightId', 'sequence'], inplace = True)

        flight_id_num = len(df.groupby(level='flightId'))
        
        new_df = pd.DataFrame(columns=['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'endDate'],
                              dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'altitude':float, 'endDate':str})

        count = 0

        for flight_id, flight_id_group in df.groupby(level='flightId'):
            
            count = count + 1
            print(airport_icao, year, month, week+1, flight_id_num, count, flight_id)
            
            flight_states_df = flight_id_group.copy()
            threshold = 0.1
            
            df_len = len(flight_states_df)
            flight_states_df.set_index('sequence', inplace=True)
            
            if not flight_states_df.empty:
                
                lats = list(flight_id_group['lat'])
                
                number_of_points = len(lats)
                
                #first lat (correct as it is inside TMA):
                prev_lat = lats[0]
                
                for i in range(0, number_of_points-1):
                    
                    next_lat = lats[i+1]
                    
                    while (lats[i]==0) | (abs(abs(lats[i]) - abs (prev_lat)) > threshold):
                        
                        lats[i] = (next_lat + prev_lat)/2
                        next_lat = lats[i]
                        
                    prev_lat = lats[i]
                    
                #last lat:
                if (lats[number_of_points-1]==0) | (abs(abs(lats[number_of_points-1]) - abs (lats[number_of_points-2])) > threshold):
                    lats[number_of_points-1] = lats[number_of_points-2]
                
                
                lons = list(flight_id_group['lon'])
                
                #first lon (correct as it is inside TMA):
                prev_lon = lons[0]
                
                for i in range(0, number_of_points-1):
                    
                    next_lon = lons[i+1]
                    
                    while (lons[i]==0) | (abs(abs(lons[i]) - abs (prev_lon)) > threshold):
                        
                        lons[i] = (next_lon + prev_lon)/2
                        next_lon = lons[i]
                        
                    prev_lon = lons[i]
                    
                #last lon:
                if (lons[number_of_points-1]==0) | (abs(abs(lons[number_of_points-1]) - abs (lons[number_of_points-2])) > threshold):
                    lons[number_of_points-1] = lons[number_of_points-2]
                    
                flight_states_df["lat"] = lats
                flight_states_df["lon"] = lons

            flight_states_df.reset_index(drop = False, inplace = True)
            flight_states_df.set_index(['flightId', 'sequence'], inplace=True)
            flight_states_df = flight_states_df[['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'endDate']]
            
            new_df = new_df.append(flight_states_df)
            
        filename = 'osn_' + airport_icao + '_states_TMA_' + year + '_' + month + '_week' + str(week + 1) + '.csv'

        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        new_df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.3f', header=False, index=True)

print((time.time()-start_time)/60)