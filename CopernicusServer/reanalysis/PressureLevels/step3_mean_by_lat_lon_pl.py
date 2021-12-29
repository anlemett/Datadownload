
year = 2019
#year = 2020

#airport_icao = 'ESSA'
airport_icao = 'ESGG'

name = '_01_06_reanalysis_pl.csv'
#name = '_07_12_reanalysis_pl.csv'


import pandas as pd

filename = 'data/' + airport_icao + '/' + airport_icao + '_' + str(year) + name


import time
start_time = time.time()

df = pd.read_csv(filename, sep=' ')

#  1        u - 'u_component_of_wind',
#  2        v - 'v_component_of_wind',


#df = df[['month','day','hour', 'level', 'latitude', 'longitude', 'u', 'v']]

df.set_index(['month', 'day', 'hour'], inplace=True)

pd.set_option('display.max_columns', None) 
#print(df.head())

df.drop('time', axis=1, inplace=True)
df.drop('latitude', axis=1, inplace=True)
df.drop('longitude', axis=1, inplace=True)

print(df.head())



month = []
day = []
hour = []
metrics = []

levels = [1, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]

column_names = ['u', 'v']
mean_df_column_names = []

for level in levels:
    for column in column_names:
        metrics.append([])
        mean_df_column_names.append(column + str(level))

for idx, hour_df in df.groupby(level=[0, 1, 2]):
    
    month.append(idx[0])
    day.append(idx[1])
    hour.append(idx[2])
    
    column_number = 0
       
    for level in levels:
    
        level_df = hour_df[hour_df['level']==level]
        
        for column in column_names:

            metrics[column_number].append(round(level_df[column].mean(),12))
            column_number = column_number + 1
                        

print(metrics)

mean_df = pd.DataFrame()
mean_df['month'] = month
mean_df['day'] = day
mean_df['hour'] = hour

print(mean_df.head())
print(mean_df_column_names)

column_number = 0
for column in mean_df_column_names:
       
    mean_df[column] = metrics[column_number]
    column_number = column_number + 1
    
    
print(mean_df.head())

filename = 'data/' + airport_icao + '/' + airport_icao + '_' + str(year) + '_mean_by_lat_lon'
mean_df.to_csv(filename, sep=' ', encoding='utf-8', float_format='%.12f', header=True, index=False)

print((time.time()-start_time)/60)
