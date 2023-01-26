
year = 2019

airport_icao = 'ENGM'


import pandas as pd

filename = 'data/' + airport_icao + '/' + airport_icao + '_' + str(year) + '_10_reanalysis_wind.csv'


import time
start_time = time.time()

df = pd.read_csv(filename, sep=' ')

rwy_df = df[df['latitude']==60]
rwy_df = rwy_df[rwy_df['longitude']==11]

filename = 'data/' + airport_icao + '/' + airport_icao + '_' + str(year) + '_10_wind_rwy.csv'
rwy_df.to_csv(filename, sep=' ', encoding='utf-8', float_format='%.12f', header=True, index=False)


#df = df[['month','day','hour', 'latitude', 'longitude', 'u100', 'v100']]
df.set_index(['month', 'day', 'hour'], inplace=True)

pd.set_option('display.max_columns', None) 
#print(df.head())

df.drop('latitude', axis=1, inplace=True)
df.drop('longitude', axis=1, inplace=True)

print(df.head())


month = []
day = []
hour = []
metrics = []


for idx, hour_df in df.groupby(level=[0, 1, 2]):
    month.append(idx[0])
    day.append(idx[1])
    hour.append(idx[2])
    
    column_number = 0
    for column in hour_df:
        
        metrics.append([])
        metrics[column_number].append(round(hour_df[column].mean(),12))
        column_number = column_number + 1

mean_df = pd.DataFrame()
mean_df['month'] = month
mean_df['day'] = day
mean_df['hour'] = hour

column_number = 0
for column in df:
    mean_df[column] = metrics[column_number]
    column_number = column_number + 1

filename = 'data/' + airport_icao + '/' + airport_icao + '_' + str(year) + '_10_wind_mean_by_lat_lon.csv'
mean_df.to_csv(filename, sep=' ', encoding='utf-8', float_format='%.12f', header=True, index=False)

print((time.time()-start_time)/60)
