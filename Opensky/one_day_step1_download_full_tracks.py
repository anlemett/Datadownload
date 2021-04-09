import os

year = '2018'

month = '05'
day = '16'

hour_begin = 5
hour_end = 6

#hour_begin = 13
#hour_end = 18

endDate = year[2:] + month + day

DATA_DIR = os.path.join("data", "one_day")
OUTPUT_DIR = os.path.join(DATA_DIR, "tracks_opensky_downloaded_" + year)


from datetime import datetime
from datetime import timezone

TMA_timezone = "Europe/Stockholm"
airport_icao = "ESSA"


import requests
import math
import pandas as pd


filename = "dropped_flights_" + year + '_' + month + '_' + day + '_' + str(hour_begin) + '_' + str(hour_end) + '.txt'
full_filename = os.path.join(OUTPUT_DIR, filename)
dropped_flights_file = open(full_filename, 'w')



from opensky_credentials import USERNAME, PASSWORD

class LiveDataRetriever:
    API_URL = 'https://opensky-network.org/api'

    AUTH_DATA = (USERNAME, PASSWORD)

    def get_list_of_arriving_aircraft(self, timestamp_begin, timestamp_end):

        arriving_flights_url = self.API_URL + '/flights/arrival'

        request_params = {
            'airport': airport_icao,
            'begin': timestamp_begin,
            'end': timestamp_end
        }

        res = requests.get(arriving_flights_url, params=request_params, auth=self.AUTH_DATA).json()
        
        for flight in res:
            print(flight['callsign'])

        return res

    def get_track_data(self, flight_icao24, flight_time):
        track_data_url = self.API_URL + '/tracks/all'

        request_params = {
            'time': flight_time,
            'icao24': flight_icao24
        }

        return requests.get(track_data_url, params=request_params, auth=self.AUTH_DATA).json()


# API does not allow longer than 7 days time periods
def get_tracks_data(data_retriever, flights, date_time_begin, date_time_end):

    number_of_flights = len(flights)

    new_data = []

    dropped_flights_icao = 0
    dropped_flights_first_seen = 0
    dropped_flights_last_seen = 0
    dropped_flights_callsign = 0

    for i in range(number_of_flights):
        print(number_of_flights, i)

        if flights[i] == 'Start after end time or more than seven days of data requested': #ESSA 22.10.2018-29.10.2018 -> 28.10 to 5th week
            print(flights[i])
            continue

        if flights[i]['icao24'] is None:
            dropped_flights_icao = dropped_flights_icao + 1
            continue

        if flights[i]['firstSeen'] is None:
            dropped_flights_first_seen = dropped_flights_first_seen + 1
            continue

        if flights[i]['lastSeen'] is None:
            dropped_flights_last_seen = dropped_flights_last_seen + 1
            continue

        while True:
            try:
                #d: icao24, startTime, endTime, callsign, path (time, latitude, longitude, baro_altitude, true_track, on_ground)
                d = data_retriever.get_track_data(flights[i]['icao24'], math.ceil((flights[i]['firstSeen']+flights[i]['lastSeen'])/2))
                break
            except Exception as str_error:
                print("Exception: ")
                print(str_error)
                pass


        sequence = 0
        
        if (flights[i]['callsign'] is None) and (d['callsign'] is None):
            dropped_flights_callsign = dropped_flights_callsign + 1
            continue
        
        for element in d['path']:
            new_d = {}

            new_d['origin'] = flights[i]['estDepartureAirport']

            new_d['sequence'] = sequence
            sequence = sequence + 1

            end_timestamp = d['endTime']
            end_datetime = datetime.utcfromtimestamp(end_timestamp)
            new_d['endDate'] = end_datetime.strftime('%y%m%d')

            el_timestamp = element[0]    #time
            el_datetime = datetime.utcfromtimestamp(el_timestamp)

            new_d['date'] = el_datetime.strftime('%y%m%d')
            new_d['time'] = el_datetime.strftime('%H%M%S')
            new_d['timestamp'] = el_timestamp
            new_d['lat'] = element[1]
            new_d['lon'] = element[2]
            new_d['baroAltitude'] = element[3]

            new_d['callsign'] = d['callsign'].strip() if d['callsign'] else flights[i]['callsign'].strip()
            new_d['icao24'] = d['icao24'].strip() if d['icao24'] else flights[i]['icao24'].strip()

            new_data.append(new_d)

    print("dropped_flights_icao", file = dropped_flights_file)
    print(dropped_flights_icao, file = dropped_flights_file)
    print("dropped_flights_first_seen", file = dropped_flights_file)
    print(dropped_flights_first_seen, file = dropped_flights_file)
    print("dropped_flights_last_seen", file = dropped_flights_file)
    print(dropped_flights_last_seen, file = dropped_flights_file)
    print("dropped_flights_callsign", file = dropped_flights_file)
    print(dropped_flights_callsign, file = dropped_flights_file)
    
    data_df = pd.DataFrame(new_data, columns = ['sequence', 'origin', 'endDate', 'callsign', 'icao24', 'date','time', 'timestamp', 'lat', 'lon', 'baroAltitude'])

    return data_df


def assign_flight_ids(tracks_df, csv_output_file):

    tracks_df['flight_id'] = tracks_df.apply(lambda row: str(row['endDate']) + str(row['callsign']), axis = 1) 
    
    tracks_df.set_index(['flight_id', 'sequence'], inplace=True)
    
    columns = ['origin']
    tracks_df.drop(columns, inplace=True, axis=1)

    tracks_df = tracks_df.groupby(level=tracks_df.index.names)
    
    tracks_df = tracks_df.first()
    
    tracks_df.to_csv(os.path.join(OUTPUT_DIR, csv_output_file), sep=' ', encoding='utf-8', float_format='%.6f', index=True, header=False)


def download_tracks(date_time_begin, date_time_end):
    
    timestamp_begin = int(date_time_begin.timestamp())   #float -> int
    timestamp_end = int(date_time_end.timestamp())

    data_retriever = LiveDataRetriever()
    flights = data_retriever.get_list_of_arriving_aircraft(timestamp_begin, timestamp_end)

    opensky_df = get_tracks_data(data_retriever, flights, date_time_begin, date_time_end)

    opensky_df = opensky_df.astype({"time": str, "date": str})
    opensky_df.reset_index(drop=True, inplace=True)
    
    filename = 'tracks_opensky_downloaded_' + year + '_' + month + '_' + day + '_' + str(hour_begin) + '_' + str(hour_end) + '.csv'
    full_filename = os.path.join(OUTPUT_DIR, filename)
    
    assign_flight_ids(opensky_df, full_filename)


import time
start_time = time.time()

DATE_TIME_BEGIN = datetime(int(year), int(month), int(day), hour_begin, 0, 0, 0, timezone.utc)

DATE_TIME_END = datetime(int(year), int(month), int(day), hour_end, 0, 0, 0, timezone.utc)

download_tracks(DATE_TIME_BEGIN, DATE_TIME_END)

dropped_flights_file.close()

print((time.time()-start_time)/60)
