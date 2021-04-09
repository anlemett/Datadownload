from datetime import datetime
from datetime import timezone

import os

import pandas as pd
import time
start_time = time.time()

airport_icao = "ESSA"
#airport_icao = "ESGG"
arrival = True

year = '2018'
month = '12'
week = 5
day = '29'

hour_begin = 10
hour_end = 13

endDate = year[2:] + month + day


OPENSKY_INPUT_DIR = os.path.join("data", "tracks_TMA_opensky_" + year)
OPENSKY_OUTPUT_DIR = os.path.join("data", "tracks_TMA_opensky_" + year)


input_filename = airport_icao + 'tracks_TMA_opensky_' + year + '_' + month + '_week' + str(week) + '.csv'
if not arrival:
    input_filename = 'departure_' + input_filename
opensky_df = pd.read_csv(os.path.join(OPENSKY_INPUT_DIR, input_filename), sep=' ',
                    names=['flight_id', 'sequence', 'origin', 'endDate', 'callsign', 'icao24', 'date', 'time', 'timestamp',
                    'lat', 'lon', 'baroAltitude'],
                    dtype={'flight_id':str, 'sequence':int, 'time':str, 'endDate':str, 'date':str, 'callsign':str, 'timestamp':int}
                    )


output_filename = airport_icao + 'tracks_TMA_opensky_' + endDate + '_' +  str(hour_begin) + '_' + str(hour_end) + '.csv'
if not arrival:
    outnput_filename = 'departure_' + output_filename

opensky_df = opensky_df[opensky_df['endDate']==endDate]

hour_begin = datetime(int(year), int(month), int(day) , hour_begin, 0, 0, 0, timezone.utc)
hour_end = datetime(int(year), int(month), int(day) , hour_end, 0, 0, 0, timezone.utc)

hour_begin_timestamp = hour_begin.timestamp()
hour_end_timestamp = hour_end.timestamp()

opensky_df = opensky_df[(opensky_df['timestamp']>=hour_begin_timestamp)&(opensky_df['timestamp']<hour_end_timestamp)]


opensky_df.to_csv(os.path.join(OPENSKY_OUTPUT_DIR, output_filename), sep=' ', encoding='utf-8', float_format='%.6f', index=False, header=None)

print((time.time()-start_time)/60)

