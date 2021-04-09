from datetime import datetime
from datetime import timezone

import os

import pandas as pd
import time
start_time = time.time()


#year = '2020'
year = '2018'

#smoothed = False
smoothed = True

airport_icao = "ESSA"
#airport_icao = "ESGG"
arrival = True

#months = ['03', '04', '05', '06', '07']
months = ['12']

month = '12'
day_begin = '29'
day_end = '29'

#month = '03'
#day_begin = '28'
#day_end = '29'

hour_begin = 10
hour_end = 13

date_begin = year[2:] + month + day_begin
date_end = year[2:] + month + day_end

if smoothed:
    ending = '_smoothed.csv'
else:
    ending = '.csv'

DATA_DIR = os.path.join("data", "states_TMA_" + year)

#input_filename = airport_icao + '_states_TMA_' + year + '_' +  months[0] + '_' + months[-1] + ending
input_filename = airport_icao + '_states_TMA_' + year + '_' +  months[0] + ending

opensky_df = pd.read_csv(os.path.join(DATA_DIR, input_filename), sep=' ',
                    names = ['flight_id', 'sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'date'],
                    dtype={'date':str, 'timestamp':int}
                    )


output_filename = airport_icao + '_states_TMA_' + date_begin + '_' +  str(hour_begin) + '_' + date_end + '_' + str(hour_end) + ending

#opensky_df = opensky_df[(opensky_df['date']>=date_begin)&(opensky_df['date']<date_end)]
opensky_df = opensky_df[opensky_df['date']==date_begin]

hour_begin = datetime(int(year), int(month), int(day_begin) , hour_begin, 0, 0, 0, timezone.utc)
hour_end = datetime(int(year), int(month), int(day_end) , hour_end, 0, 0, 0, timezone.utc)

hour_begin_timestamp = hour_begin.timestamp()
hour_end_timestamp = hour_end.timestamp()

opensky_df = opensky_df[(opensky_df['timestamp']>=hour_begin_timestamp)&(opensky_df['timestamp']<hour_end_timestamp)]


opensky_df.set_index(['flight_id', 'sequence'], inplace = True)
for flight_id, flight_id_group in opensky_df.groupby(level='flight_id'):
    print(flight_id)



opensky_df.to_csv(os.path.join(DATA_DIR, output_filename), sep=' ', encoding='utf-8', float_format='%.6f', index=True, header=True)


# For GPS Visualizer (https://www.gpsvisualizer.com/)

def getNewTrack(trackpoint):
    if trackpoint == 0:
        return 1
    else:
        return 0

def getType():
    return 'T'


#opensky_df.reset_index(drop=False, inplace=True)

#opensky_df = opensky_df[['flight_id', 'sequence', 'lat', 'lon', 'altitude']]
#opensky_df = opensky_df[['flight_id', 'sequence', 'lat', 'lon']]
#opensky_df = opensky_df.rename(columns={'flight_id': 'name', 'sequence': 'trackpoint'})


#opensky_df['type'] = opensky_df.apply(lambda row: getType(), axis=1)

#opensky_df['new_track'] = opensky_df.apply(lambda row: getNewTrack(row['trackpoint']), axis=1)

#output_filename = 'gps_visualizer__' + output_filename
#opensky_df.to_csv(os.path.join(DATA_DIR, output_filename), sep=',', encoding='utf-8', float_format='%.6f', index=False, header=True)

print((time.time()-start_time)/60)

