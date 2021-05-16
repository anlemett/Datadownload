##############################################################################

#airport_icao = "ESSA"
airport_icao = "ESGG"
arrival = True

year = '2019'

#months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
months = ['03', '04', '05', '06', '07']
#months = ['04']

##############################################################################

import os

DATA_DIR = os.path.join("data", airport_icao)
DATA_DIR = os.path.join(DATA_DIR, year)
DATA_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_TMA_" + year)


import pandas as pd
import time
start_time = time.time()

frames = []
#opensky_states_df = pd.DataFrame(columns=['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'endDate', 'aircraftType'])

for month in months:
    print(year, month)

    filename = 'osn_' + airport_icao + '_states_TMA_' + year + '_' + month + '.csv'
    if not arrival:
        filename = 'departure_' + filename
    
    df = pd.read_csv(os.path.join(DATA_DIR, filename), sep=' ', names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'endDate'],
                     dtype = str)
     
    frames.append(df)

opensky_states_df = pd.concat(frames)

filename = 'osn_' + airport_icao + '_states_TMA_' + year + '_' +  months[0] + '_' + months[-1] + '.csv'
if not arrival:
    filename = 'departure_' + filename
    

opensky_states_df.to_csv(os.path.join(DATA_DIR, filename), sep=' ', encoding='utf-8', float_format='%.6f', index=False, header=None)

print((time.time()-start_time)/60)
