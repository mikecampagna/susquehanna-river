import h5py
import numpy as np
import os
import tifffile as tf
from collections import deque
import gdal
from random import randint;

os.chdir('E:/RWD_nat/tiffs/')
rd8 = 'E:\\RWD_nat\\tiffs\\MAFDR\\fdr1.tif'
gord8 = gdal.Open(rd8)
gord81 = gord8.GetRasterBand(1)
gord81sub = gord81.ReadAsArray(1,1,22982,28379)

## Example to create a memory map
d8 = np.memmap(filename = 'E:\\RWD_nat\\memmp\\d8.mmp', dtype='int64', mode='w+', shape=(gord81sub.shape[0]+4,gord81sub.shape[1]+4,1));
 

d8[2:gord81sub.shape[0]+2, 2:gord81sub.shape[1]+2, 0] = gord81sub 





















 