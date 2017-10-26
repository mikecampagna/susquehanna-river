import h5py
import numpy as np
import os
import tifffile as tf
from collections import deque
import gdal
from random import randint;
import math;

rname = 'G:\\WilliamPenn_Share\\EDS\\SUSQUEHANNA\\DATA\\USGS\\NLCD_HR\\IR.tif'
ropen = gdal.Open(rname)

rxdim = ropen.RasterXSize
rydim = ropen.RasterYSize

rxdimf = math.floor(rxdim/3)
rydimf = math.floor(rydim/3)
ropenb1 = ropen.GetRasterBand(1)
ropenb1sub = ropenb1.ReadAsArray()

## Example to create a memory map
mmp = np.memmap(filename = 'E:\\RWD_nat\\memmp\\b.mmp', dtype='int16', mode='w+', shape=(rydimf,rxdimf,13));

for x in range(0,rxdim-1):
    for y in range(0,rydim-1):
        mmp[y,x,0] = sum(sum(ropenb1sub[(3*y):(3*y)+2,(3*x):(3*x)+2]));





















 