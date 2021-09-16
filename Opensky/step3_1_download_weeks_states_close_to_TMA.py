##############################################################################

#airport_icao = "ESSA"
#airport_icao = "ESGG"
#airport_icao = "EIDW" # Dublin
airport_icao = "LOWW" # Vienna

arrival = True

year = "2019"

#months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
months = ['10']

##############################################################################

import os

DATA_DIR = os.path.join("data", airport_icao)
DATA_DIR = os.path.join(DATA_DIR, year)
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_tracks_TMA_" + year)
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_close_to_TMA_" + year)

if not os.path.exists(INPUT_DIR):
    os.makedirs(INPUT_DIR)
    
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    

from datetime import datetime
import pytz

from opensky_credentials import USERNAME, PASSWORD

import paramiko
from io import StringIO
import re

import numpy as np
import pandas as pd
import calendar

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon


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
                password=PASSWORD#,
                #timeout=120,
                #look_for_keys=False,
                #allow_agent=False,
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
    
    #print("request_states", icao24)

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


    request = "select time, lat, lon, baroaltitude, velocity from state_vectors_data4 where icao24=\'" + icao24 + "\' and time>=" + time_begin_str + " and time<=" + time_end_str + " and hour>=" + hour_begin_str + " and hour<=" + hour_end_str + ";\n"
    
    #print(request)
    while not shell.send_ready():
        time.sleep(1)

    shell.send(request)


    while not shell.recv_ready():
        time.sleep(1)
    
    total = ""
    count = 0
    while len(total) == 0 or total[-10:] != ":21000] > ":
        #print("inside while", count)
        count = count + 1
        b = shell.recv(256)
        #print("after shell.recv")
        total += b.decode()
        #print("after b.decode")
        #print(total)

    impala_log = StringIO(total)
    
    #print("total")
    #print(total)
    #print("impala_log")
    #print(impala_log)

    return get_df(impala_log, time_end)


def download_states_week(month, week):
    
    client = connectToImpala()

    shell = client.invoke_shell()
    
    getShellReady(shell)
    
    
    # opensky tracks csv
    opensky_tracks_filename = 'osn_' + airport_icao + '_tracks_TMA_' + year + '_' + month + '_week' + str(week) + '.csv'
    if not arrival:
        opensky_tracks_filename = 'departure_' + opensky_tracks_filename

    #opensky states inside TMA csv
    opensky_states_filename = 'osn_' + airport_icao + '_states_close_to_TMA_' + year + '_' + month + '_week' + str(week) + '.csv'
    if not arrival:
        opensky_states_filename = 'departure_' + opensky_states_filename

    opensky_states_df = pd.DataFrame()

    opensky_tracks_df = pd.read_csv(os.path.join(INPUT_DIR, opensky_tracks_filename), sep=' ',
                                names=['flightId', 'sequence', 'origin', 'endDate', 'callsign', 'icao24', 'date', 'time', 'timestamp',
                                    'lat', 'lon', 'baroAltitude'],
                                index_col=[0,1],
                                dtype={'flightId':str, 'sequence':int, 'icao24': str, 'timestamp':int})


    number_of_flights = len(opensky_tracks_df.groupby(level='flightId'))

    count = 0
    
    print("Downloading")

    for flight_id, new_df in opensky_tracks_df.groupby(level='flightId'):
        count = count + 1
    
        print(airport_icao, year, month, week, number_of_flights, count)
    
        (id, first_index) = new_df.index[0]
        (id, last_index) = new_df.index[-1]


        icao24 = new_df.loc[(flight_id, first_index)]['icao24']
        time_begin = new_df.loc[(flight_id, first_index)]['timestamp']
        time_end = new_df.loc[(flight_id, last_index)]['timestamp']
        
        flight_id_states_df = request_states(shell, icao24, time_begin, time_end)

        if flight_id_states_df is not None and not flight_id_states_df.empty:

            flight_id_states_df.insert(0, 'flight_id', flight_id)

            flight_id_states_df.set_index(['flight_id'], inplace=True)

            opensky_states_df = pd.concat([opensky_states_df, flight_id_states_df], axis=0, sort=False)
    
    print("Fixing")
    # fix "time" inserted
    opensky_states_df = opensky_states_df[opensky_states_df.timestamp != "time"]
    opensky_states_df = opensky_states_df.astype({"timestamp": int})
    #opensky_states_df["timestamp"] = pd.to_numeric(opensky_states_df["timestamp"])
    
    # sort timestamps and reassign sequence
    
    flight_id_num = len(opensky_states_df.groupby(level='flight_id'))
    count = 0
    
    new_states_df = pd.DataFrame()
    
    for flight_id, flight_id_group in opensky_states_df.groupby(level='flight_id'):
        
        count = count + 1
        print(airport_icao, year, month, week, flight_id_num, count)
        
        flight_id_group_length = len(flight_id_group)
        
        sequence_list = list(range(flight_id_group_length))
        
        new_flight_df = flight_id_group.sort_values(by=['timestamp'])
        
        new_flight_df = new_flight_df.drop(['sequence'], axis=1)
        
        new_flight_df['sequence'] = sequence_list

        new_flight_df = new_flight_df[['sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'endDate']]
        
        new_states_df = new_states_df.append(new_flight_df)
        
    new_states_df.to_csv(os.path.join(OUTPUT_DIR, opensky_states_filename), sep=' ', encoding='utf-8', float_format='%.6f', header=None, index = True)
    
    closeConnection(client, shell)


import time
start_time = time.time()

from multiprocessing import Process


for month in months:

    procs = [] 
        
    number_of_weeks = (5, 4)[month == '02' and not calendar.isleap(int(year))]
        
    for week in range(0, number_of_weeks):
        
        proc = Process(target=download_states_week, args=(month, week + 1,))
        procs.append(proc)
        proc.start()
        
    # complete the processes
    for proc in procs:
        proc.join()

print((time.time()-start_time)/60)
