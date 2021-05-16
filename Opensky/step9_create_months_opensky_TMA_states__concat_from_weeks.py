##############################################################################

airport_icao = "ESSA"
#airport_icao = "ESGG"
#airport_icao = "EIDW" # Dublin
#airport_icao = "LOWW" # Vienna

arrival = True

year = '2019'

months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']


##############################################################################

import os

DATA_DIR = os.path.join("data", airport_icao)
DATA_DIR = os.path.join(DATA_DIR, year)
DATA_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_TMA_" + year)

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

import pandas as pd
import calendar

import time
start_time = time.time()

for month in months:
    print(airport_icao, year, month)

    opensky_states_df = pd.DataFrame(columns=['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'endDate'],
                                     dtype=str)
    
    number_of_weeks = (5, 4)[month == '02' and not calendar.isleap(int(year))]
        
    for week in range(0, number_of_weeks):

        filename = 'osn_' + airport_icao + '_states_TMA_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        if not arrival:
            filename = 'departure_' + filename
    
        df = pd.read_csv(os.path.join(DATA_DIR, filename), sep=' ', names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'endDate'],
                     dtype = str)
        
        opensky_states_df = opensky_states_df.append(df, ignore_index=True)
        
    opensky_states_df = opensky_states_df.drop_duplicates(['flightId','sequence'],keep= 'first')

    filename = 'osn_' + airport_icao + '_states_TMA_' + year + '_' + month + '.csv'
    if not arrival:
        filename = 'departure_' + filename
    opensky_states_df.to_csv(os.path.join(DATA_DIR, filename), sep=' ', encoding='utf-8', float_format='%.6f', index=False, header=None)

print((time.time()-start_time)/60)
