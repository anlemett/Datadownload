import xarray as xr
from math import sqrt
import pandas as pd

year = 2020
filename = 'data/arlanda/arlanda_' + str(year) + '_summer_reanalysis.'

# Missing data: some rows in cbh, i10fg (fill with mean)


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
#print(df.head())


df['month'] = df.apply(lambda row: getMonth(row['time']), axis=1)

df['day'] = df.apply(lambda row: getDay(row['time']), axis=1)

df['hour'] = df.apply(lambda row: getHour(row['time']), axis=1)

df['wind'] = df.apply(lambda row: getWind(row['u10'], row['v10']), axis=1)


df = df[['month','day','hour', 'latitude', 'longitude', 'i10fg', 'wind', 'cbh', 'lcc', 'tcc', 'cape', 'cp', 'tp']]
#df = df[['month','day','hour', 'latitude', 'longitude', 'tp']]
df = df.sort_values(by = ['month', 'day', 'hour', 'latitude', 'longitude'], ascending = [True, True, True, True, False])

print(df.head())

df.set_index(['month','day','hour', 'latitude', 'longitude'], inplace=True)
df = df[~df.index.duplicated(keep='first')]

df.reset_index( inplace=True)

print(df.head())

df = df.fillna(df.mean())

df.to_csv(filename + 'csv', sep=' ', encoding='utf-8', float_format='%.6f', header=True, index=False)

print((time.time()-start_time)/60)