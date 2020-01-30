#!/usr/bin/env python3




import  os, argparse,multiprocessing, time, traceback, shutil, json, math, datetime
import numpy as np
from pympc import utils
from pympc.generate_tiles import getTileIndex, getTileName



def argument_parser():


    parser = argparse.ArgumentParser(description="""This script is used to distribute the points of a bunch of LAS/LAZ files in
different tiles. The XY extent of the different tiles match the XY extent of the
nodes of a certain level of a octree defined by the provided bounding box (z
is not required by the XY tiling). Which level of the octree is matched
depends on specified number of tiles:
 - 4 (2X2) means matching with level 1 of octree
 - 16 (4x4) means matching with level 2 of octree
 and so on. """)
    #parser.add_argument('-i','--input',default='',help='Input data folder (with LAS/LAZ files)',type=str, required=True)
    parser.add_argument('-o','--output',default='',help='Output data folder for the different tiles',type=str, required=True)
    parser.add_argument('-t','--temp',default='',help='Temporal folder where required processing is done',type=str, required=True)
    parser.add_argument('-e','--extent',default='',help='XY extent to be used for the tiling, specify as "minX minY maxX maxY". maxX-minX must be equal to maxY-minY. This is required to have a good extent matching with the octree',type=str, required=True)
    parser.add_argument('-n','--number',default='',help='Number of tiles (must be the power of 4. Example: 4, 16, 64, 256, 1024, etc.)',type=int, required=True)
    parser.add_argument('-p','--proc',default=1,help='Number of processes [default is 1]',type=int)

    parser.add_argument('-cf','--dlconfigfile',default=None,help='File path to download run config file for which downloadLists are to be merged.')
    parser.add_argument('-nvm','--numbervms',default=1,help='number of vms on which retiling of given input is being run in parallel.')
    parser.add_argument('-v','--vmj',default=0,help='numerical id of vm script is running on')

    return parser




def main():

    args = argument_parser().parse_args()
    with open('out.out','w+') as of:
        print(args.output,file=of)
        print(args.temp,file=of)
        print(args.extent,file=of)
        print(args.number,file=of)
        print(args.proc,file=of)
        print(args.dlconfigfile,file=of)
        print(args.numbervms,file=of)
        print(args.vmj,file=of)







if __name__ == '__main__':
    main()
