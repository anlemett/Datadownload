#!/usr/bin/env python
from ecmwfapi import ECMWFDataServer

server = ECMWFDataServer(url="https://api.ecmwf.int/v1",key="492b442f04f7909ada9b51394b16cc5f",email="anastasia.lemetti@liu.se")

server.retrieve({
    "class": "yp",
    "dataset": "yopp",
    "expver": "1",
    "levtype": "sfc",
    "stream": "enfo",
    "type": "cf",
    
    "param": "20.3", #visibility
    
    "date": "2020-04-01/to/2020-04-30", # change date 
    
    'area': '61/17/59/19',
    "grid": "0.5/0.5",
    
    #"time": "00:00:00",
    "time": "00:00:00/12:00:00",
    
    #"step": "24",
    "step": "12",
    
    "format" : "netcdf",
    "target": "data/visibility_arlanda_2020_04.nc", # change name !
})

