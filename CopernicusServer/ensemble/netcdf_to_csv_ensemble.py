import xarray as xr
import pandas as pd
import math

##############################################################################

# 11.2019 and 07.2020

year = 2020

month = 7

airport = "Kiruna"
#airport = "Malmo"
#airport = "Ovik"
#airport = "Sundsvall"
#airport = "Umeo"


metric = 'i10fg'	# +

#metric = 'sf'		# +

#metric = 'tp'		# +


#metric = 'wind'


# use netcdf_to_csv_ensemble_2_metrics.py for cbh, lcc, cape and cp
#metric = 'cbh'

#metric = 'lcc'

#metric = 'cape'

#metric = 'cp'


##############################################################################

nc_filename = 'data/' + airport + '/' + airport + '_' + str(year) + '_ensemble.nc'
csv_filename = 'data/' + airport + '/' + airport + '_' + metric + '_' + str(year) + '_' + str(month)+ '_ensemble.csv'


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

def getWindSpeed(u, v):
    return math.sqrt(u**2+v**2)


import time
start_time = time.time()


DS = xr.open_dataset(nc_filename)

df = DS.to_dataframe()

df.reset_index(inplace=True)



df['month'] = df.apply(lambda row: getMonth(row['time']), axis=1)

df['day'] = df.apply(lambda row: getDay(row['time']), axis=1)

df['hour'] = df.apply(lambda row: getHour(row['time']), axis=1)

df['date'] = df.apply(lambda row: getDate(row['month'], row['day']), axis=1)


if metric == "wind":

    df['wind'] = df.apply(lambda row: getWindSpeed(row['u10'], row['v10']), axis=1)



pd.set_option('display.max_column',None)
#print(df.head())


cols = ['month','day','hour', 'date', 'number'] + [metric]

df = df[cols]
df['number'] = df['number'].astype(int)

df = df.sort_values(by = ['month', 'day', 'hour'], ascending = [True, True, True])


df = df[df.month == month]

print(df.head())

table = pd.pivot_table(df, values=metric, index=['month','day','hour', 'date'],
                    columns=['number'])



table.to_csv(csv_filename, sep=' ', encoding='utf-8', float_format='%.9f', header=True, index=True)

print((time.time()-start_time)/60)
