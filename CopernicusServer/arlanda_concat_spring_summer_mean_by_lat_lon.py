
import pandas as pd

year = 2020

import time
start_time = time.time()

filename_spring = 'data/arlanda/arlanda_' + str(year) + '_spring_mean_by_lat_lon.csv'
filename_summer = 'data/arlanda/arlanda_' + str(year) + '_summer_mean_by_lat_lon.csv'


df_spring = pd.read_csv(filename_spring, sep=' ')
df_summer = pd.read_csv(filename_summer, sep=' ')

df = pd.concat([df_spring, df_summer], axis=0)

pd.set_option('display.max_columns', None) 
print(df.head())


df.to_csv('data/arlanda/arlanda_' + str(year) + '_spring_summer_mean_by_lat_lon.csv', sep=' ', float_format='%.6f', encoding='utf-8', index = False, header=True, )


print((time.time()-start_time)/60)