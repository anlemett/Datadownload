##############################################################################

#airport_icao = "ESSA"
#airport_icao = "ESGG"
#airport_icao = "EIDW" # Dublin
airport_icao = "LOWW" # Vienna

year = '2019'
month = '10'
week = 1

##############################################################################

import os

DATA_DIR = os.path.join("data", airport_icao)
DATA_DIR = os.path.join(DATA_DIR, year)

DIR = os.path.join(DATA_DIR, "osn_" + airport_icao + "_states_TMA_" + year)

import pandas as pd

        
filename1 = 'osn_' + airport_icao + '_states_TMA_' + year + '_' + month + '_week' + str(week) + '.csv'
        
full_filename1 = os.path.join(DIR, filename1)
        
        
df1 = pd.read_csv(full_filename1, sep=' ',
                 names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'endDate'],
                 dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'altitude':float, 'endDate':str})

df1.set_index(['flightId'], inplace = True)

flight_id_num1 = len(df1.groupby(level='flightId'))


filename2 = 'osn_' + airport_icao + '_states_TMA_' + year + '_' + month + '_week' + str(week) + '_test.csv'
        
full_filename2 = os.path.join(DIR, filename2)
        
        
df2 = pd.read_csv(full_filename2, sep=' ',
                 names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'endDate'],
                 dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'altitude':float, 'endDate':str})

df2.set_index(['flightId'], inplace = True)

flight_id_num2 = len(df2.groupby(level='flightId'))


idx1 = df1.index
idx2 = df2.index
#print(idx2)
dif1 = idx1.difference(idx2)
print(dif1)
dif2 = idx2.difference(idx1)
print(dif2)
