##############################################################################

airport_icao = "ESSA"
#airport_icao = "ESGG"
#airport_icao = "EIDW" # Dublin
#airport_icao = "LOWW" # Vienna

departure = True

year = '2019'

#months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
months = ['10']

##############################################################################

import os

DATA_DIR = os.path.join("data", airport_icao)
DATA_DIR = os.path.join(DATA_DIR, year)

INPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_TMA_smoothed_" + year)
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_TMA_" + year)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


# flights with the last altitude value less than this value are considered as 
# landed and with complete data
descent_end_altitude = 600 #meters

climb_first_altitude = 1000 # meters
climb_end_altitude = 2000 #meters

import pandas as pd
import numpy as np
import calendar


import time
start_time = time.time()


for month in months:
    print(month)
    
    number_of_weeks = (5, 4)[month == '02' and not calendar.isleap(int(year))]
    
    #for week in range(0, number_of_weeks):
    for week in range(0, 1):
        
        print(airport_icao, year, month, week+1)
        
        filename = airport_icao + '_states_TMA_smoothed_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        
        if departure:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_' + filename

        
        full_filename = os.path.join(INPUT_DIR, filename)
        
        df = pd.read_csv(full_filename, sep=' ',
            names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'beginDate', 'endDate'],
            dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'altitude':int, 'beginDate':str, 'endDate':str})
        
        
        #new_df = pd.DataFrame(columns=['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'beginDate', 'endDate'],
        #    dtype=str)
        
        df.set_index(['flightId', 'sequence'], inplace = True)
        
        flight_id_num = len(df.groupby(level='flightId'))
        count = 0
        
        for flight_id, flight_id_group in df.groupby(level='flightId'):
            
            count = count + 1
            print(airport_icao, year, month, week+1, flight_id_num, count, flight_id)
            
            if departure:
                
                ###################################################################
                # Last altitude too small (incomplete data or bad smoothing):
                    ###################################################################
                    
                altitudes = flight_id_group['altitude']
                last_height = altitudes.tolist()[-1]
                
                if last_height < climb_end_altitude:
                    df = df.drop(flight_id)
                    continue
                
                ###################################################################
                # First altitude too big (incomplete data):
                
                altitudes = flight_id_group['altitude']
                first_height = altitudes.tolist()[0]
                
                if first_height > climb_first_altitude:
                    df = df.drop(flight_id)
                    continue
            else:
                ###################################################################
                # Last altitude too big (incomplete data or bad smoothing):
                    ###################################################################
                    
                altitudes = flight_id_group['altitude']
                last_height = altitudes.tolist()[-1]
                
                if last_height > descent_end_altitude:
                    df = df.drop(flight_id)
                    continue
                
                ###################################################################
                # First altitude too small (departure and arrival at the same airport):
                ###################################################################
                
                altitudes = flight_id_group['altitude']
                first_height = altitudes.tolist()[0]
                
                if first_height < descent_end_altitude:
                    df = df.drop(flight_id)
                    continue
                    
        filename = airport_icao + '_states_TMA_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        
        if departure:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_' + filename

        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=False, index=True)

print((time.time()-start_time)/60)