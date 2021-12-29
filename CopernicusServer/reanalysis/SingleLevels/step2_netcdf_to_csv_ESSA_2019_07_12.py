
year = 2019

airport_icao = 'ESSA'
#airport_icao = 'ESGG'


import xarray as xr
from math import sqrt
import pandas as pd

filename = 'data/' + airport_icao + '/' + airport_icao + '_' + str(year) + '_07_12_reanalysis.'


def getMonth(pandas_timestamp):
    return pandas_timestamp.to_pydatetime().month

def getDay(pandas_timestamp):
    return pandas_timestamp.to_pydatetime().day

def getHour(pandas_timestamp):
    return pandas_timestamp.to_pydatetime().hour

def getWind(u, v):

    return sqrt(u**2+v**2)


import time
start_time = time.time()


nc_filename = filename + 'nc'
DS = xr.open_dataset(nc_filename)

df = DS.to_dataframe()

df.reset_index(inplace=True)

pd.set_option('display.max_columns', None) 
print(df.head())



df['month'] = df.apply(lambda row: getMonth(row['time']), axis=1)

df['day'] = df.apply(lambda row: getDay(row['time']), axis=1)

df['hour'] = df.apply(lambda row: getHour(row['time']), axis=1)

df['wind10'] = df.apply(lambda row: getWind(row['u10'], row['v10']), axis=1)
df['wind100'] = df.apply(lambda row: getWind(row['u100'], row['v100']), axis=1)



#df = df[['month','day','hour', 'latitude', 'longitude', 'i10fg', 'wind', 'cbh', 'lcc', 'tcc', 'cape', 'cp', 'tp', 'sf', 'sd']]


df = df.sort_values(by = ['month', 'day', 'hour', 'latitude', 'longitude'], ascending = [True, True, True, True, False])

df.to_csv(filename + 'csv', sep=' ', encoding='utf-8', float_format='%.12f', header=True, index=False)

print((time.time()-start_time)/60)