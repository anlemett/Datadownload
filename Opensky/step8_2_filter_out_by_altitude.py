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

# flights with the last altitude value less than this value are considered as 
# landed and with complete data
descent_end_altitude = 600 #meters, ?make less?

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
        
        filename = 'osn_' + airport_icao + '_states_50NM_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        
        full_filename = os.path.join(INPUT_DIR, filename)
        
        df = pd.read_csv(full_filename, sep=' ',
                                 names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'beginDate', 'endDate'],
                                 dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'altitude':float, 'velocity':int, 'beginDate':str, 'endDate':str})
        
        
        new_df = pd.DataFrame(columns=['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'beginDate', 'endDate'],
                              dtype=str)
        
        df.set_index(['flightId', 'sequence'], inplace = True)
        
        flight_id_num = len(df.groupby(level='flightId'))
        count = 0
        
        for flight_id, flight_id_group in df.groupby(level='flightId'):
            
            count = count + 1
            print(airport_icao, year, month, week+1, flight_id_num, count, flight_id)
            
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
        
        filename = 'osn_' + airport_icao + '_states_50NM_' + year + '_' + month + '_week' + str(week + 1) + '.csv'

        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=False, index=True)

print((time.time()-start_time)/60)