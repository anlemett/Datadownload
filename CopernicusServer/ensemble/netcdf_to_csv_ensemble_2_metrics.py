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


metric1 = 'cbh'

metric2 = 'lcc'

#metric1 = 'cape'

#metric2 = 'cp'

##############################################################################

nc_filename = 'data/' + airport + '/' + airport + '_' + str(year) + '_ensemble.nc'
csv_filename = 'data/' + airport + '/' + airport + '_' + metric1 + '_' + metric2 + '_' + str(year) + '_' + str(month)+ '_ensemble.csv'


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


DS = xr.open_dataset(nc_filename)

df = DS.to_dataframe()

df.reset_index(inplace=True)



df['month'] = df.apply(lambda row: getMonth(row['time']), axis=1)

df['day'] = df.apply(lambda row: getDay(row['time']), axis=1)

df['hour'] = df.apply(lambda row: getHour(row['time']), axis=1)

df['date'] = df.apply(lambda row: getDate(row['month'], row['day']), axis=1)



pd.set_option('display.max_column',None)
#print(df.head())


cols = ['month','day','hour', 'date', 'number'] + [metric1, metric2]

df = df[cols]
df['number'] = df['number'].astype(int)

df = df.sort_values(by = ['month', 'day', 'hour'], ascending = [True, True, True])


df = df[df.month == month]


table1 = pd.pivot_table(df, values=metric1, index=['month','day','hour', 'date'],
                    columns=['number'])

columns = []
for i in range(0,10):
    columns.append(metric1 + str(i))
print(columns)
table1.columns = columns


table2 = pd.pivot_table(df, values=metric2, index=['month','day','hour', 'date'],
                    columns=['number'])

columns = []
for i in range(0,10):
    columns.append(metric2 + str(i))
table2.columns = columns

df = pd.concat([table1, table2], axis=1)
df.to_csv(csv_filename, sep=' ', encoding='utf-8', float_format='%.9f', header=True, index=True)

print((time.time()-start_time)/60)
