from datetime import datetime
from datetime import timezone
import pytz


import os

year = '2018'

month = '05'
day = '16'

hour_begin = 5
hour_end = 6


DATE_TIME_BEGIN = datetime(int(year), int(month), int(day), hour_begin, 0, 0, 0, timezone.utc)
DATE_TIME_END = datetime(int(year), int(month), int(day), hour_end, 0, 0, 0, timezone.utc)

timestamp_begin = int(DATE_TIME_BEGIN.timestamp())
timestamp_end = int(DATE_TIME_END.timestamp())


DATA_DIR = os.path.join("data", "one_day")
INPUT_DIR = os.path.join(DATA_DIR, "tracks_TMA_opensky_" + year)
OUTPUT_DIR = os.path.join(DATA_DIR, "states_close_to_TMA_" + year)


from opensky_credentials import USERNAME, PASSWORD

import paramiko
from io import StringIO
import re


import pandas as pd


def get_df(impala_log, time_end):
    s = StringIO()
    count = 0
    for line in impala_log.readlines():
        
        line = line.strip()
        if re.match("\|.*\|", line):
            count += 1
            s.write(re.sub(" *\| *", ",", line)[1:-2])
            s.write("\n")

    #contents = s.getvalue()
    #print(contents)

    if count > 0:
        s.seek(0)
        df = pd.read_csv(s, sep=',', error_bad_lines=False, warn_bad_lines=True)

        df = df.fillna(0)
        df.index = df.index.set_names(['sequence'])
        df.columns = ['timestamp', 'lat', 'lon', 'altitude', 'velocity']
        df[['lat', 'lon']] = df[['lat', 'lon']].apply(pd.to_numeric, downcast='float', errors='coerce').fillna(0)
        df[['altitude', 'velocity']] = df[['altitude', 'velocity']].apply(pd.to_numeric, downcast='integer', errors='coerce').fillna(0)
        df['altitude'] = df['altitude'].astype(int)
        df['velocity'] = df['velocity'].astype(int)
        end_datetime = datetime.utcfromtimestamp(time_end)
        df['endDate'] = end_datetime.strftime('%y%m%d')

        df.reset_index(level=df.index.names, inplace=True)
        return df


def connectToImpala():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    while True:
        try:
            print("trying to connect")
            client.connect(
                hostname = 'data.opensky-network.org',
                port=2230,
                username=USERNAME,
                password=PASSWORD,
                timeout=60,
                look_for_keys=False,
                allow_agent=False,
                #compress=True
                )
            break
        except paramiko.SSHException as err:
            print(err)
            time.sleep(2)
        except:
            print("exception")
            time.sleep(2)

    return client


def getShellReady(shell):
    while not shell.recv_ready():
        time.sleep(1)

    total = ""
    while len(total) == 0 or total[-10:] != ":21000] > ":
        b = shell.recv(256)
        total += b.decode()



def closeConnection(client, shell):
    shell.close()
    client.close()


#Set in this function which fields to extract (in sql request)
def request_states(shell, icao24, time_begin, time_end):
    time_begin_datetime = datetime.utcfromtimestamp(time_begin)
    time_begin_datetime = time_begin_datetime.replace(tzinfo=pytz.timezone('UTC'))
    hour_begin_datetime = time_begin_datetime.replace(microsecond=0,second=0,minute=0)
    hour_begin_timestamp = int(datetime.timestamp(hour_begin_datetime))
    
    time_end_datetime = datetime.utcfromtimestamp(time_end)
    time_end_datetime = time_end_datetime.replace(tzinfo=pytz.timezone('UTC'))
    hour_end_datetime = time_end_datetime.replace(microsecond=0,second=0,minute=0)
    hour_end_timestamp = int(datetime.timestamp(hour_end_datetime))

    time_begin_str = str(time_begin)
    time_end_str = str(time_end)
    hour_begin_str = str(hour_begin_timestamp)
    hour_end_str = str(hour_end_timestamp)

    while not shell.send_ready():
        time.sleep(1)

    request = "select time, lat, lon, baroaltitude, velocity from state_vectors_data4 where icao24=\'" + icao24 + "\' and time>=" + time_begin_str + " and time<=" + time_end_str + " and hour>=" + hour_begin_str + " and hour<=" + hour_end_str + ";\n"
    #print(request)
    shell.send(request)
    total = ""

    while not shell.recv_ready():
        time.sleep(1)

    while len(total) == 0 or total[-10:] != ":21000] > ":
        while not shell.recv_ready():
            time.sleep(1)
        b = shell.recv(256)
        total += b.decode()

    impala_log = StringIO(total)
    
    return get_df(impala_log, time_end)


import time
start_time = time.time()


# opensky tracks csv
opensky_tracks_filename = 'tracks_TMA_opensky_' + year + '_' + month + '_' + day + '_' + str(hour_begin) + '_' + str(hour_end) + '.csv'

#opensky states inside TMA csv
opensky_states_filename = 'states_close_to_TMA_' + year + '_' + month + '_' + day + '_' + str(hour_begin) + '_' + str(hour_end) + '.csv'

opensky_states_df = pd.DataFrame()

opensky_tracks_df = pd.read_csv(os.path.join(INPUT_DIR, opensky_tracks_filename), sep=' ',
                                names=['flightId', 'sequence', 'endDate', 'callsign', 'icao24', 'date', 'time', 'timestamp',
                                    'lat', 'lon', 'baroAltitude'],
                                index_col=[0,1],
                                dtype={'flightId':str, 'sequence':int, 'icao24': str, 'timestamp':int})



client = connectToImpala()

shell = client.invoke_shell()

getShellReady(shell)


number_of_flights = len(opensky_tracks_df.groupby(level='flightId'))

count = 0

for flight_id, flight_df in opensky_tracks_df.groupby(level='flightId'):
    count = count + 1
    
    (id, last_index) = flight_df.index[-1]

    icao24 = flight_df.loc[(flight_id, 0)]['icao24']

    flight_id_states_df = request_states(shell, icao24, timestamp_begin, timestamp_end)

    if flight_id_states_df is not None and not flight_id_states_df.empty:
        
        print(number_of_flights, count)
        
        flight_id_states_df.insert(0, 'flight_id', flight_id)
        flight_id_states_df.set_index(['flight_id', 'sequence'], inplace=True)

        # fix "time" inserted
        flight_id_states_df = flight_id_states_df[flight_id_states_df.timestamp != "time"]
        
        for id, flight_id_group in flight_id_states_df.groupby(level='flight_id'):
            
            flight_id_group_length = len(flight_id_group)
            
            sequence_list = list(range(flight_id_group_length))
            
            flight_id_states_df.loc[id, :] = flight_id_group.sort_values(by=['timestamp'])
            
            flight_id_states_df.loc[id, 'sequence'] = sequence_list
            
        opensky_states_df = pd.concat([opensky_states_df, flight_id_states_df], axis=0, sort=False)
        
        opensky_states_df.to_csv(os.path.join(OUTPUT_DIR, opensky_states_filename), sep=' ', encoding='utf-8', float_format='%.3f', header=None)
        
    else:
        print(number_of_flights, count, "empty")


shell.close()
client.close()

print((time.time()-start_time)/60)
