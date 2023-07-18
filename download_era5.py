#!/usr/bin/env python

#from cdsapi import ECMWFDataServer
import cdsapi
import os,sys
import datetime
from time import time
from threading import Thread
from queue import Queue




def era5_atm(yyyymmddhh):
    yyyy = str(yyyymmddhh[0:4])
    mm   = str(yyyymmddhh[4:6])
    dd   = str(yyyymmddhh[6:8])
    hh   = str(yyyymmddhh[8: ])
    filename = os.path.join(yyyymmddhh[:4], yyyymmddhh[:8], "era5-atm-" + yyyymmddhh + ".nc")
    print(filename)
    server   = cdsapi.Client()
    server.retrieve('reanalysis-era5-pressure-levels',{
        'product_type': 'reanalysis',
        "pressure_level": ['1000', '925', '850', '700', '600', '500', '400', '300', '250', '200', '150', '100', '50'],
        "variable"  : ['geopotential', 'specific_humidity', 'temperature', 'u_component_of_wind', 'v_component_of_wind'],
        "date"   : yyyy+mm+dd,
        "time"   : hh,
        "area"   : [90, 0, -90, 359.75],
        "format" : "netcdf",
        }, filename)
def era5_sfc(yyyymmddhh):
    yyyy = yyyymmddhh[0:4]
    mm   = yyyymmddhh[4:6]
    dd   = yyyymmddhh[6:8]
    hh   = yyyymmddhh[8: ]
    filename = os.path.join(yyyymmddhh[:4], yyyymmddhh[:8], "era5-sfc-" + yyyymmddhh + ".nc")
    print(filename)
    server   = cdsapi.Client()
    server.retrieve('reanalysis-era5-single-levels',{
        'product_type': 'reanalysis',
        'variable': ['mean_sea_level_pressure', '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_temperature'],
        "date"   : yyyy+mm+dd,
        "time"   : hh,
        "area"   : [90, 0, -90, 359.75],
        "format" : "netcdf",
        }, filename)


class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
    def run(self):
        while True:
            date= self.queue.get()
#            era5_atm(date)
            era5_sfc(date)
            self.queue.task_done()




ipath  = './'

begin = datetime.datetime(1979, 1, 1)
end   = datetime.datetime(2023, 1, 1)
delta = datetime.timedelta(hours = 1)

links = []

idate = begin
while idate < end:
    yyyymmddhh = idate.strftime("%Y%m%d%H")
    yyyymmdd   = idate.strftime("%Y%m%d")
    yyyy       = idate.strftime("%Y")
    path = os.path.join(ipath, yyyy, yyyymmdd)
    if not os.path.exists(path):
        os.makedirs(path)
    links.append(str(yyyymmddhh))
    idate += delta
    print(yyyymmddhh)
queue = Queue()
for x in range(24):
    worker =DownloadWorker(queue)
    worker.daeon = True
    worker.start()
for link in links:
    queue.put((link))
queue.join()

