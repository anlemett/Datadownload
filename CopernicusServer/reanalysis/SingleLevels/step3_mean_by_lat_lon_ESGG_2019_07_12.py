
year = 2019

#airport_icao = 'ESSA'
airport_icao = 'ESGG'


import pandas as pd

filename = 'data/' + airport_icao + '/' + airport_icao + '_' + str(year) + '_07_12_reanalysis.csv'


import time
start_time = time.time()

df = pd.read_csv(filename, sep=' ')

#  1        u100 - '100m_u_component_of_wind',
#  2        v100 - '100m_v_component_of_wind',
#  3        u10 - '10m_u_component_of_wind',
#  4        v10 - '10m_v_component_of_wind',
#  5        cbh - 'cloud_base_height',
#  6        cape - 'convective_available_potential_energy',
#  7        cin -  'convective_inhibition',
#  8        cp - 'convective_precipitation',
#  9        csf - 'convective_snowfall',
#  10        csdr -'convective_snowfall_rate_water_equivalent',
#  11       hcc - 'high_cloud_cover', 
#  12        i10fg - 'instantaneous_10m_wind_gust',
#  13       kx - 'k_index',
#  14        lsf - 'large_scale_snowfall',
#  15        lssfr - 'large_scale_snowfall_rate_water_equivalent',
#  16        lcc - 'low_cloud_cover',
#  17        mcc - 'medium_cloud_cover',
#          ptype - 'precipitation_type',
#  18        sf - 'snowfall',
#  19        tcc - 'total_cloud_cover',
#  20        tciw - 'total_column_cloud_ice_water',
#  21        tclw - 'total_column_cloud_liquid_water',
#  22       tcrw - 'total_column_rain_water',
#  23        tcsw - 'total_column_snow_water',
#  24        tcw - 'total_column_water',
#  25        tp - 'total_precipitation',
#          wind10
#          wind100

#df = df[['month','day','hour', 'latitude', 'longitude', 'i10fg', 'wind', 'cbh', 'lcc', 'tcc', 'cape', 'cp', 'tp', 'sf', 'sd']]
df.set_index(['month', 'day', 'hour'], inplace=True)

pd.set_option('display.max_columns', None) 
#print(df.head())

df.drop('time', axis=1, inplace=True)
df.drop('latitude', axis=1, inplace=True)
df.drop('longitude', axis=1, inplace=True)
df.drop('ptype', axis=1, inplace=True)

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

filename = 'data/' + airport_icao + '/' + airport_icao + '_' + str(year) + '_07_12_mean_by_lat_lon.csv'
mean_df.to_csv(filename, sep=' ', encoding='utf-8', float_format='%.12f', header=True, index=False)

print((time.time()-start_time)/60)
