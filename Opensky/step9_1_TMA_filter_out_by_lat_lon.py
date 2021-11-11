##############################################################################

airport_icao = "ESSA"
#airport_icao = "ESGG"
#airport_icao = "EIDW" # Dublin
#airport_icao = "LOWW" # Vienna

departure = True
# this step might be skipped for departure

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

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

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


for month in months:
    print(month)
    
    number_of_weeks = (5, 4)[month == '02' and not calendar.isleap(int(year))]
    #number_of_weeks = 1
        
    #for week in range(0, number_of_weeks):
    for week in range(0, 1):
        
        print(airport_icao, year, month, week+1)
        
        filename = airport_icao + '_states_TMA_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        
        if departure:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_' + filename
        
        full_filename = os.path.join(INPUT_DIR, filename)
        
        
        df = pd.read_csv(full_filename, sep=' ',
            names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity',  'beginDate', 'endDate'],
            dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'altitude':int, 'beginDate':str, 'endDate':str})

        df.set_index(['flightId', 'sequence'], inplace = True)

        flight_id_num = len(df.groupby(level='flightId'))
        count = 0

        for flight_id, flight_id_group in df.groupby(level='flightId'):
            
            count = count + 1
            print(airport_icao, year, month, week+1, flight_id_num, count, flight_id)
            
            ###################################################################
            # Latitude or longitude outside of TMA too much
            ###################################################################
            
            lon_min = min(TMA_lon) - 0.5
            lon_max = max(TMA_lon) + 0.5
            lat_min = min(TMA_lat) - 0.5
            lat_max = max(TMA_lat) + 0.5
            
            rect_lon = [lon_min, lon_min, lon_max, lon_max, lon_min]
            rect_lat = [lat_min, lat_max, lat_max, lat_min, lat_min]
            
            lons_lats_vect = np.column_stack((rect_lon, rect_lat)) # Reshape coordinates
            polygon = Polygon(lons_lats_vect) # create polygon
            
            flight_states_df = flight_id_group.copy() 
            
            flight_states_df.reset_index(drop = False, inplace = True)
            df_len = len(flight_states_df)
            flight_states_df.set_index('sequence', inplace=True)
            
            remove = 0
            if not flight_states_df.empty:
                
                for seq, row in flight_states_df.iterrows():
                    
                    point = Point(row["lon"], row["lat"])
                    
                    if not polygon.contains(point):
                        remove = 1
                        break
                        
            if remove == 1:
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
