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
DATA_DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_TMA_raw_" + year)

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

import pandas as pd
import calendar

import time
start_time = time.time()

def getCallsign(flight_id):
    
    return flight_id[6:]



for month in months:
    print(month)
    
    number_of_weeks = (5, 4)[month == '02' and not calendar.isleap(int(year))]
        
    for week in range(0, number_of_weeks):
        
        print(airport_icao, year, month, week+1)
        
        filename = 'osn_' + airport_icao + '_states_TMA_raw_all_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        if not arrival:
            filename = 'departure_' + filename
        
        full_filename = os.path.join(DATA_DIR, filename)
        
        
        df = pd.read_csv(full_filename, sep=' ',
                                 names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'endDate'],
                                 dtype=str)
        
        # Remove the following calsigns:
        
        # consists of only letters (Swedish police helicopters: SEMIX, SEXTD, SEJP, SEJPN, SEJPX, SEJPO, ...)
        
        df['callsign'] = df.apply(lambda row: getCallsign(row['flightId']), axis=1)
        df = df[~df["callsign"].str.isalpha()]
        
        # consists of only digits (Scandinavian Air Ambulance)
        
        df = df[~df["callsign"].str.isdigit()]
        
        # starting with DFL ((Babcock Scandinavian Air Ambulance)
        # (contains 'DFL' also works)
        
        searchfor = ['DFL']
        
        df = df[~df.flightId.str.contains('|'.join(searchfor))]
        
        # starting with SVF (Swedish Armed Forces )
        # (contains 'SVF' also works)
        
        searchfor = ['SVF']
        
        df = df[~df.flightId.str.contains('|'.join(searchfor))]
        
        # starting with HMF (Swedish Maritime Administration )
        # (contains 'HMF' also works)
        
        searchfor = ['HMF']
        
        df = df[~df.flightId.str.contains('|'.join(searchfor))]
        
        # TODO: remove freight traffic ?
        
        
        df = df.drop('callsign', 1)
        
        
        filename = 'osn_' + airport_icao + '_states_TMA_raw_' + year + '_' + month + '_week' + str(week + 1) + '.csv'
        if not arrival:
            filename = 'departure_' + filename
        
        full_filename = os.path.join(DATA_DIR, filename)
        
        df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.3f', header=False, index=False)

print((time.time()-start_time)/60)
