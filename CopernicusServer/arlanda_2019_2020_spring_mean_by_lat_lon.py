import pandas as pd

year = 2020
filename = 'data/arlanda/arlanda_' + str(year) + '_spring_reanalysis.csv'


import time
start_time = time.time()

df = pd.read_csv(filename, sep=' ')

pd.set_option('display.max_columns', None) 
print(df.head())


df = df[['month','day','hour', 'latitude', 'longitude', 'i10fg', 'wind', 'cbh', 'lcc', 'tcc', 'cape', 'cp', 'tp']]
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


filename = 'data/arlanda/arlanda_' + str(year) + '_spring_mean_by_lat_lon.csv'
mean_df.to_csv(filename, sep=' ', encoding='utf-8', float_format='%.6f', header=True, index=False)

print((time.time()-start_time)/60)
