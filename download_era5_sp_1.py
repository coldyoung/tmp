#!/usr/bin/env python

#from cdsapi import ECMWFDataServer
import cdsapi
import os,sys
import datetime
from time import time
from threading import Thread
from queue import Queue




def era5_sfc(yyyymmddhh):
    yyyy = yyyymmddhh[0:4]
    mm   = yyyymmddhh[4:6]
    dd   = yyyymmddhh[6:8]
    hh   = yyyymmddhh[8: ]
    filename = os.path.join(yyyymmddhh[:4], yyyymmddhh[:8], "era5-sp-" + yyyymmddhh + ".nc")
    print(filename)
    server   = cdsapi.Client()
    server.retrieve('reanalysis-era5-single-levels',{
        'product_type': 'reanalysis',
        'variable': ['surface_pressure'],
        "date"   : yyyy+mm+dd,
        "time"   : '0/to/23/by/1',
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
            era5_sfc(date)
            self.queue.task_done()




ipath  = './'

begin = datetime.datetime(1979, 1, 1)
end   = datetime.datetime(1979, 1, 2)
delta = datetime.timedelta(days = 1)

links = []

idate = begin
while idate < end:
    yyyymmddhh = idate.strftime("%Y%m%d%H")
    yyyymmdd   = idate.strftime("%Y%m%d")
    yyyy       = idate.strftime("%Y")
    path = os.path.join(ipath, yyyy, yyyymmdd)
    if not os.path.exists(path):
        os.makedirs(path)
    links.append(str(yyyymmdd))
    idate += delta
    print(yyyymmdd)
queue = Queue()
for x in range(24):
    worker =DownloadWorker(queue)
    worker.daeon = True
    worker.start()
for link in links:
    queue.put((link))
queue.join()

