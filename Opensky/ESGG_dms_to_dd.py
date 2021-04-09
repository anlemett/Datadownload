# Waypoints lat, lon in degrees/minutes/seconds
waypoints_dms = {
'ELBUX' : '573318.6N 0115836.7E',
'GG410' :'575008.2N 0122616.2E',
'GG411' : '575204.9N 0123142.6E',
'GG412' : '574845.8N 0123552.2E',
'GG413' : '574616.0N 0123721.3E',
'GG414' : '575337.2N 0122549.6E',
'GG415' : '575320.8N 0121821.0E',
'GG416' : '575647.3N 0122628.5E',
'GG417' : '575914.6N 0122756.8E',
'GG418' : '580941.8N 0123414.9E',
'GG419' : '575838.0N 0122028.4E',
'GG420' : '575708.6N 0123244.1E',
'GG421' : '575821.0N 0123418.3E',
'GG422' : '580721.9N 0124606.4E',
'GG607' : '573549.6N 0124331.7E',
'GG608' : '571249.3N 0122734.4E',
'GG708' : '572921.2N 0120721.8E',
'GG709' : '572552.2N 0120749.9E',
'GG710' : '572119.2N 0121445.0E',
'GG711' : '570935.3N 0120030.1E',
'GG712' : '572027.9N 0120313.1E',
'GG713' : '572624.4N 0120442.9E',
'GG714' : '571917.1N 0114011.3E',
'GG715' : '572206.2N 0114101.5E',
'GG716' : '571930.6N 0115236.2E',
'GG717' : '572025.9N 0115922.2E',
'GG718' : '572723.5N 0120200.0E',
'GG719' : '573029.1N 0115805.5E',
'GG720' : '573337.0N 0115955.2E',
'GG721' : '573641.6N 0120143.2E',
'GG903' : '575053.2N 0121554.1E',
'GG904' : '574110.9N 0120619.0E',
'GG905' : '575216.9N 0121352.2E',
'GG906' : '574750.3N 0115519.8E',
'GG907' : '580809.9N 0121013.0E',
'GG908' : '574436.4N 0120901.8E',
'GG909' : '574631.0N 0121047.3E'
}


# no sign because of 'N' and 'E'
def dms2dd(as_string):
    degrees = int(as_string[:2])
    minutes = int(as_string[2:4])
    seconds = float(as_string[4:8])
    lat_dd = float(degrees) + float(minutes)/60 + float(seconds)/(60*60);
    degrees = int(as_string[10:13])
    minutes = int(as_string[13:15])
    seconds = float(as_string[15:19])
    lon_dd = float(degrees) + float(minutes)/60 + float(seconds)/(60*60);

    return lat_dd, lon_dd;


f = open("ESGG_waypoints.py", "w")

for key, value in waypoints_dms.items():
    lat, lon = dms2dd(value)
    f.write(key)
    f.write("_lat = ")
    f.write(str(lat))
    f.write('\n')
    f.write(key)
    f.write("_lon = ")
    f.write(str(lon))
    f.write('\n')
f.close()

    
    
    
    
    