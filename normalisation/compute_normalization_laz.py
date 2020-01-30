import argparse
import time
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('path_of_laserchicken',help='Path to laserchicken')
parser.add_argument('input',help='absolute input path (point cloud)')
parser.add_argument('radius',help='side length of normalization cell')
parser.add_argument('output',help='abolute path to output pointcloud')

args=parser.parse_args()

import sys
sys.path.insert(0,args.path_of_laserchicken)

from laserchicken import read_laz_for_normalization as rl
from laserchicken import laz_add_normalized_height as lanh
from laserchicken import write_laz_for_normalization as wl
from laserchicken.keys import point
from laserchicken.normalization import normalize

print("------ Import started -------")
imstart = time.time()


pc, las = rl.read(args.input)

imend = time.time()
imdiff = imend - imstart
print("------ Import completed -----")

print("------ Starting normalization ----")
start_time = time.time()

pc = normalize(pc, np.float(args.radius))

las = lanh.add_normalized_height(las,pc)

end_time = time.time()
difftime2 = end_time - start_time

wstart = time.time()
wl.write(las,args.output)
wend = time.time()
wdiff = wend - wstart

print(("import completed in : %f sec") % (imdiff))
print(("normalization completed in: %f sec") % (difftime2))
print(("output serialization completed in : %f sec") % (wdiff))


                
