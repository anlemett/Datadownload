#airport_icao = "ESSA"
#airport_icao = "ESGG"
#airport_icao = "EIDW" # Dublin
airport_icao = "LOWW" # Vienna

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from datetime import datetime
from datetime import timezone

from constants import *
if airport_icao == "ESSA":
    from constants_ESSA import *
elif airport_icao == "ESGG":
    from constants_ESGG import *
elif airport_icao == "EIDW":
    from constants_EIDW import *
elif airport_icao == "LOWW":
    from constants_LOWW import *

def make_TMA_plot():

    #plt.plot(TMA_lon, TMA_lat, color="blue")
    #plt.plot(rwy1_lon,rwy1_lat, color="red")
    #plt.plot(rwy2_lon,rwy2_lat, color="red")
    #plt.plot(rwy3_lon,rwy3_lat, color="red")
    #plt.plot(rwy4_lon,rwy4_lat, color="red")

    #plt.plot(HMR_lon, HMR_lat, 'ro')
    #plt.plot(NILUG_lon, NILUG_lat, 'ro')
    #plt.plot(XILAN_lon, XILAN_lat, 'ro')
    #plt.plot(ELTOK_lon, ELTOK_lat, 'ro')
    plt.plot(TMA_lon, TMA_lat, color='r', linewidth=2)
    

def plot_vertical_profile(df, linewidth, altitude_fluctuation_threshold):
    
    plt.xlabel('Time [sec]', fontsize=25)
    plt.ylabel('Altitude [m]', fontsize=25)
    
    plt.tick_params(labelsize=15)
    
    for flight_id, flight_id_group in df.groupby(level='flight_id'):
        flight_states_opensky_df = df.loc[(flight_id,), :]
                
        if not flight_states_opensky_df.empty:
            opensky_states_altitudes = []
            opensky_states_times = []
            opensky_states_fixed_altitudes = []

            t = 0
            prev_altitude = flight_states_opensky_df.head(1)['altitude'].item()
            
            #if prev_altitude < 3000:
            #    continue

            for seq, row in flight_states_opensky_df.groupby(level='sequence'):
            
                t = t + 1
                opensky_states_times.append(t)
                        
                opensky_states_altitudes.append(row['altitude'].item())
            
                if abs(row['altitude'].item() - prev_altitude) > altitude_fluctuation_threshold:     #to remove altitude fluctuations
                    opensky_states_fixed_altitudes.append(prev_altitude)
                    continue
            
                opensky_states_fixed_altitudes.append(row['altitude'].item())

                prev_altitude = row['altitude'].item()
            
            plt.plot(opensky_states_times, opensky_states_fixed_altitudes, color='r', linewidth=linewidth)


def plot_horizontal_profile(df, linewidth):
    
    plt.xlabel('Longitude', fontsize=25)
    plt.ylabel('Latitude', fontsize=25)
    
    plt.tick_params(labelsize=15)
    
    for flight_id, new_df in df.groupby(level='flight_id'):
        print(flight_id)
        flight_states_opensky_df = df.loc[(flight_id,), :]
        lon = []
        lat = []
        for seq, row in flight_states_opensky_df.groupby(level='sequence'):
        
            if row['lon'].item() != 0:
                lon.append(row['lon'].item())
            if row['lat'].item() != 0:
                lat.append(row['lat'].item())
            
        plt.plot(lon, lat, color='r', linewidth=linewidth)



#filename = "data/states_TMA_2020/test_states_TMA_2020_05_week1.csv"

#df = pd.read_csv(filename, sep=' ',
#                 names = ['flight_id', 'sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'date'])

#df.set_index(['flight_id', 'sequence'], inplace = True)

#print(df.head())


#plot_vertical_profile(df, 2, 100)

make_TMA_plot()

#plot_horizontal_profile(df, 2)



