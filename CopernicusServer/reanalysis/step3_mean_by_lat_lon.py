
year = 2019

airport_icao = 'ESSA'
#airport_icao = 'ESGG'


import pandas as pd

filename = 'data/' + airport_icao + '/' + airport_icao + '_' + str(year) + '_reanalysis.csv'


import time
start_time = time.time()

df = pd.read_csv(filename, sep=' ')

pd.set_option('display.max_columns', None) 
print(df.head())

#          u100 - '100m_u_component_of_wind',
#          v100 - '100m_v_component_of_wind',
#          u10 - '10m_u_component_of_wind',
#          v10 - '10m_v_component_of_wind',
#          cbh - 'cloud_base_height',
#          cape - 'convective_available_potential_energy',
#          cin -  'convective_inhibition',
#          cp - 'convective_precipitation',
#          csf - 'convective_snowfall',
#          csdr -'convective_snowfall_rate_water_equivalent',
#          hcc - 'high_cloud_cover', 
#          i10fg - 'instantaneous_10m_wind_gust',
#          kx - 'k_index',
#          lsf - 'large_scale_snowfall',
#          lssfr - 'large_scale_snowfall_rate_water_equivalent',
#          lcc - 'low_cloud_cover',
#          mcc - 'medium_cloud_cover',
#          ptype - 'precipitation_type',
#          sf - 'snowfall',
#          tcc - 'total_cloud_cover',
#          tciw - 'total_column_cloud_ice_water',
#          tclw - 'total_column_cloud_liquid_water',
#          tcrw - 'total_column_rain_water',
#          tcsw - 'total_column_snow_water',
#          tcw - 'total_column_water',
#          tp - 'total_precipitation',
#          wind10
#          wind100

df = df[['month','day','hour', 'latitude', 'longitude', 'i10fg', 'wind', 'cbh', 'lcc', 'tcc', 'cape', 'cp', 'tp', 'sf', 'sd']]
df.set_index(['month', 'day', 'hour'], inplace=True)

month = []
day = []
hour = []
mean_gust = []
mean_wind = []
mean_cbh = []
mean_lcc = []
mean_tcc = []
mean_cape = []
mean_cp = []
mean_tp = []
mean_sf = []
mean_sd = []

for idx, hour_df in df.groupby(level=[0, 1, 2]):
    month.append(idx[0])
    day.append(idx[1])
    hour.append(idx[2])
    mean_gust.append(round(hour_df['i10fg'].mean(),3))
    mean_wind.append(round(hour_df['wind'].mean(),3))
    mean_cbh.append(round(hour_df['cbh'].mean(),3))
    mean_lcc.append(round(hour_df['lcc'].mean(),3))
    mean_tcc.append(round(hour_df['tcc'].mean(),3))
    mean_cape.append(round(hour_df['cape'].mean(),3))
    mean_cp.append(round(hour_df['cp'].mean(),3))
    mean_tp.append(round(hour_df['tp'].mean(),3))
    mean_sf.append(round(hour_df['sf'].mean(),3))
    mean_sd.append(round(hour_df['sd'].mean(),3))
    

mean_df = pd.DataFrame()
mean_df['month'] = month
mean_df['day'] = day
mean_df['hour'] = hour
mean_df['gust'] = mean_gust
mean_df['wind'] = mean_wind
mean_df['cbh'] = mean_cbh
mean_df['lcc'] = mean_lcc
mean_df['tcc'] = mean_tcc
mean_df['cape'] = mean_cape
mean_df['cp'] = mean_cp
mean_df['tp'] = mean_tp
mean_df['sf'] = mean_sf
mean_df['sd'] = mean_sd


filename = 'data/' + airport_icao + '/' + airport_icao + '_' + str(year) + '_mean_by_lat_lon.csv'
mean_df.to_csv(filename, sep=' ', encoding='utf-8', float_format='%.12f', header=True, index=False)

print((time.time()-start_time)/60)
