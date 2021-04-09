import xarray as xr

year = 2020
month = '04'


def getMonth(pandas_timestamp):
    return pandas_timestamp.to_pydatetime().month

def getDay(pandas_timestamp):
    return pandas_timestamp.to_pydatetime().day

def getHour(pandas_timestamp):
    return pandas_timestamp.to_pydatetime().hour


def getDate(month, day):
    year_str = str(year)
    month_str = str(month) if month>9 else "0"+str(month)
    day_str = str(day) if day>9 else "0"+str(day)
    return year_str + month_str + day_str

import time
start_time = time.time()


filename = 'data/visibility_arlanda_' + str(year) + '_' + month
nc_filename = filename + '.nc'
DS = xr.open_dataset(nc_filename)

df = DS.to_dataframe()

df.reset_index(inplace=True)



df['month'] = df.apply(lambda row: getMonth(row['time']), axis=1)

df['day'] = df.apply(lambda row: getDay(row['time']), axis=1)

df['hour'] = df.apply(lambda row: getHour(row['time']), axis=1)

df['date'] = df.apply(lambda row: getDate(row['month'], row['day']), axis=1)



#print(df.head())
df = df[['month','day','hour', 'date', 'latitude', 'longitude', 'p3020']]
df.rename(columns={'p3020': 'visibility'}, inplace=True)

df = df.sort_values(by = ['month', 'day', 'hour', 'latitude', 'longitude'], ascending = [True, True, True, True, False])

csv_filename = filename + '.csv'

df.to_csv(csv_filename, sep=' ', encoding='utf-8', float_format='%.3f', header=True, index=False)

print((time.time()-start_time)/60)
