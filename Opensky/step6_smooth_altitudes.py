##############################################################################

#airport_icao = "ESSA"
airport_icao = "ESGG"
#airport_icao = "EIDW" # Dublin
#airport_icao = "LOWW" # Vienna

arrival = True

year = '2021'

#months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
months = ['01', '02', '03']

##############################################################################

import os

DATA_DIR = os.path.join("data", airport_icao)
DATA_DIR = os.path.join(DATA_DIR, year)
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_TMA_raw_" + year)
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_TMA_" + year)

if not os.path.exists(INPUT_DIR):
    os.makedirs(INPUT_DIR)
    
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)



import pandas as pd
import calendar
from scipy.ndimage import gaussian_filter1d

import time
start_time = time.time()


for month in months:
    print(month)
    
    number_of_weeks = (5, 4)[month == '02' and not calendar.isleap(int(year))]
        
    for week in range(0, number_of_weeks):
        
        print(airport_icao, year, month, week+1)
        
        filename = 'osn_' + airport_icao + '_states_TMA_raw_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        if not arrival:
            filename = 'departure_' + filename
        
        full_filename = os.path.join(INPUT_DIR, filename)
        
        
        df = pd.read_csv(full_filename, sep=' ',
                                 names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'velocity', 'endDate'],
                                 dtype={'sequence':int, 'timestamp':int, 'altitude':int, 'endDate':str})
        
        #df = df[df["altitude"]>final_approach_altitude]

        new_df = pd.DataFrame(columns=['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude',
                                       'velocity', 'endDate'],
                              dtype=str)

        df.set_index(['flightId', 'sequence'], inplace = True)

        flight_id_num = len(df.groupby(level='flightId'))
        count = 0

        for flight_id, flight_id_group in df.groupby(level='flightId'):
            
            count = count + 1
            print(airport_icao, year, month, week+1, flight_id_num, count, flight_id)
            
            flight_states_df = flight_id_group.copy()
            #flight_states_df = df.loc[(flight_id, ), :]
            
            # PHASE 1 Substitute big fluctuations with neigbor values
            
            altitude_fluctuation_threshold = 1000 # todo? make different thresholds for up and down fluctuations (up < down)
            
            flight_states_df.reset_index(drop = False, inplace = True)
            df_len = len(flight_states_df)
            flight_states_df.set_index('sequence', inplace=True)
            
            #flight_states_df =flight_states_df.sort_index(level=['sequence'], ascending = False)
            
            if not flight_states_df.empty:

                opensky_states_altitudes = []
                opensky_states_times = []
                opensky_states_fixed_altitudes = []

                t = 0
                prev_altitude = list(flight_states_df['rawAltitude'])[0] # Caution: if first altitude is wrong (fluctuation), it will break
                                                                         # the whhole trajectory
                
                for seq, row in flight_states_df.iterrows():
                    
                    t = t + 1
                    opensky_states_times.append(t)
                    
                    opensky_states_altitudes.append(row['rawAltitude'])
                    
                    if abs(row['rawAltitude'] - prev_altitude) > altitude_fluctuation_threshold:
                        opensky_states_fixed_altitudes.append(prev_altitude)
                        continue
                    
                    opensky_states_fixed_altitudes.append(row['rawAltitude'])
                    
                    prev_altitude = row['rawAltitude']
                    
                flight_states_df["altitude"] = opensky_states_fixed_altitudes
                
            flight_states_df.reset_index(drop = False, inplace = True)
            flight_states_df.set_index(['flightId', 'sequence'], inplace=True)
            
            # PHASE 2 Use Gaussian filter to smooth 'stairs'
            
            y = list(flight_states_df["altitude"])
            
            # Smooth with a gaussian filter
            smooth_y = gaussian_filter1d(y, 10)
            
            flight_states_df["altitude"] = smooth_y
            
            flight_states_df = flight_states_df.reset_index(drop=False)
            
            flight_states_df = flight_states_df[['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'endDate']]
            
            new_df = new_df.append(flight_states_df)
            
            #print(flight_states_df.head(1))
            
            #print(new_df.head(1))
        
        
        filename = 'osn_' + airport_icao + '_states_TMA_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        if not arrival:
            filename = 'departure_' + filename
        
        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        new_df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.3f', header=False, index=False)

print((time.time()-start_time)/60)

