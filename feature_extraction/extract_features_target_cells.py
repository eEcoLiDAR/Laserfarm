#!/usr/bin/env python3


"""
This script runs laserchicken (from the specified path), extracting a user defined list of features from the normalized input tiles for a list of user defined targets (per tile). Currently these are cell based, but that can be adapted in principle
""" 



import argparse
import time
import numpy as np
import sys


default_features = ['point_density', 'std_z', 'var_z', 'skew_z', 'kurto_z', 'entropy_z','std_norm_z','var_norm_z', 'skew_norm_z', 'kurto_norm_z', 'entropy_norm_z', 'max_norm_z', 'min_norm_z', 'range_norm_z', 'mean_norm_z', 'median_norm_z', 'coeff_var_norm_z','density_absolute_mean_norm_z', 'pulse_penetration_ratio','perc_10_z','perc_20_z', 'perc_40_z', 'perc_60_z', 'perc_80_z', 'perc_90_z','perc_10_norm_z','perc_20_norm_z', 'perc_40_norm_z', 'perc_60_norm_z', 'perc_80_norm_z', 'perc_90_norm_z']
    
parser = argparse.ArgumentParser()
parser.add_argument('-p','--path_of_laserchicken',help='the path of laserchicken version to use')
parser.add_argument('-i','--datafile',help='absolute path of point cloud data file')
parser.add_argument('-t','--targetfile',help='laz file (point cloud) of targets associated with input data file')
parser.add_argument('-r','--radius',help='radius of cell; length of side for rectangular volumes')
parser.add_argument('-o','--outputfile',help='absolute path of output pointcloud file')
parser.add_argument('-ex','--features',default=default_features, help='list of features to extract')
parser.add_argument('-fa','--filterAttribute',default=None,help='attribute in input point cloud file to filter on')
parser.add_argument('-fv','--filterValue',default=None,help='value of filter attrribute to select')

args = parser.parse_args()

sys.path.insert(0, args.path_of_laserchicken)

from laserchicken import read_laz
from laserchicken.keys import point
from laserchicken.volume_specification import Cell
from laserchicken.compute_neighbors import compute_neighborhoods
from laserchicken.feature_extractor import compute_features
from laserchicken.write_ply import write
from laserchicken.select import select_equal

imstart=time.time()

pc_nonfiltered = read_laz.read(args.datafile,norm=True)
target = read_laz.read(args.targetfile)

imend=time.time()
imdiff= imend-imstart
print(('import completed in % sec') % (imdiff))

print(("Number of points: %s ") % (pc_nonfiltered[point]['x']['data'].shape[0]))
print(("Number of points in target: %s ") % (target[point]['x']['data'].shape[0]))

if args.filterAttribute != None :
        
        
    filt_att = args.filterAttribute.strip()
    filt_att_val = int(args.filterValue.strip())

    print("-----Filtering input pointcloud -----")
    print(("Filtering points on %s ") % args.filterAttribute)
    print(("Selecting points with %s equal to %s") % (filt_att, filt_att_val))
    pc = select_equal(pc_nonfiltered, filt_att, filt_att_val)
    print(("Number of filtered points: %s ") % (pc[point]['x']['data'].shape[0]))

else:

    print('no filtering requested')
    pc = pc_nonfiltered


print("------ Computing neighborhood is started ------")

start = time.time()

neighbors=compute_neighborhoods(pc, target, Cell(np.float(args.radius)))
iteration=0
target_idx_base=0
for x in neighbors:
    end = time.time()
    difftime=end - start
    print(("build kd-tree: %f sec") % (difftime))
    print("Computed neighborhoods list length at iteration %d is: %d" % (iteration,len(x)))

    start1 = time.time()
    print("------ Feature calculation is started ------")

    compute_features(pc, x, target_idx_base, target, args.features, Cell(np.float(args.radius)))

    target_idx_base+=len(x)
    end1 = time.time()
    difftime1=end1 - start1
    print(("feature calc: %f sec") % (difftime1))
    iteration+=1


write(target, args.outputfile)
