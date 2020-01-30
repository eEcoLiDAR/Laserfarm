#!/usr/bin/env python3
"""

This script creates regularly gridded target files at specified resolution 
for a regular grid of specifed extent based on the actual tiles present.

"""

import argparse, time, math, os, pylas
import numpy as np
#from laserchicken.test_tools import create_point_cloud

def getTileCoordFromName(name):
    """
    parse file name to obtain tile coordiantes
    """
    
    tXcoord = name.split('_')[1]
    tYcoord = name.split('_')[2]
    
    return np.int(tXcoord), np.int(tYcoord)



def getTileBounds(tXcoord,tYcoord,minX,minY,maxX,maxY,axisTiles):
    """
    use tile coordinates and grid extent to identify boreder of each tile
    """

    #minX = gridExtent[0]
    #minY = gridExtent[1]
    #maxX = gridExtent[2]
    #maxY = gridExtent[3]
    #axisTiles = gridExtent[4]
    
    #tminX = np.float(np.int((tXcoord*(maxX - minX)/axisTiles) + minX))
    #tminY = np.float(np.int((tYcoord*(maxY - minY)/axisTiles) + minY))
    tminX = np.floor((tXcoord*(maxX - minX)/axisTiles) + minX)
    tminY = np.ceil((tYcoord*(maxY - minY)/axisTiles) + minY)

    tmaxX = tminX + ((maxX - minX)/axisTiles)
    tmaxY = tminY + ((maxY - minY)/axisTiles)
    
    
    return tminX, tminY, tmaxX, tmaxY



def getNumberCellsGridPoints(tminX,tminY,tmaxX,tmaxY,targetCellSize):
    
    nXcells = getNumberCellsDim(tminX,tmaxX,targetCellSize)
    nYcells = getNumberCellsDim(tminY,tmaxY,targetCellSize)
    nGridPoints = nXcells*nYcells

    return nXcells, nYcells, nGridPoints

    
    
    
def getNumberCellsDim(mindim,maxdim,cellsize):
    """ get number cells along a dimension"""
    return max(int(np.ceil((maxdim - mindim)/float(cellsize))),1)





def setTargetPCvalues(tminX,tminY,tmaxX,tmaxY,targetCellSize,nXcells,nYcells,nGridPoints):

    xvals = [tminX + targetCellSize*(0.5 + (i % nXcells)) for i in range(nGridPoints)]
    yvals = [tminY + targetCellSize*(0.5 + np.floor(i / nYcells)) for i in range(nGridPoints)]
    zvals = np.zeros_like(xvals)

    return np.array(xvals), np.array(yvals), np.array(zvals)


def outputTargetLaz(name,outputPath,xv,yv,zv):

    targets = pylas.create(file_version="1.2",point_format_id=3)
    #targets.header.scales=np.array([0.01,0.01,0.01])
    #targets.header.offsets=np.array([0.0,0.0,0.0])
    targets.header.x_offset = 0.0
    targets.header.y_offset = 0.0
    targets.header.z_offset = 0.0
    targets.header.x_scale = 0.01
    targets.header.y_scale = 0.01
    targets.header.z_scale = 0.01
    targets.X = (xv*100).astype(int)
    targets.Y = (yv*100).astype(int)
    targets.Z = (zv*100).astype(int)

    outfilename=outputPath+'/'+name+'_targets.laz'

    targets.write_to_file(outfilename,do_compress=True)

    return outfilename


    

def createTargetGrid(name,outputPath,targetCellSize,minX,minY,maxX,maxY,axisTiles):

    tXcoord, tYcoord = getTileCoordFromName(name)

    print('tXcoord : ', tXcoord)
    print('tYcoord : ', tYcoord)
    
    tminX,tminY,tmaxX,tmaxY = getTileBounds(tXcoord,tYcoord,minX,minY,maxX,maxY,axisTiles)

    print('tminX : ', tminX)
    print('tminY : ', tminY)
    print('tmaxX : ', tmaxX)
    print('tmaxY : ', tmaxY)
    
    
    nXcells, nYcells, nGridPoints = getNumberCellsGridPoints(tminX,tminY,tmaxX,tmaxY,targetCellSize)

    print('nXcells : ', nXcells)
    print('nYcells : ', nYcells)
    print('nGridPoints : ', nGridPoints)
    
    xv, yv, zv = setTargetPCvalues(tminX,tminY,tmaxX,tmaxY,targetCellSize,nXcells,nYcells,nGridPoints)    

    ofn = outputTargetLaz(name,outputPath,xv,yv,zv)

    print('created '+ofn)




def argument_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('-i','--inputdir', default='.',help='input directory')
    parser.add_argument('-o','--outputdir',default='.',help='output directory')
    parser.add_argument('-g','--gridextent',default='',help='grid extent of tiling scheme and number of tiles along 1 dimension')
    parser.add_argument('-t','--targetcellsize',default='',help='cell size of target cells (in m)')
    return parser




def parse_extent(argpExt):
    gridExtent=[]

    Xmin, Ymin, Xmax, Ymax, axisTiles = argpExt.split(' ')

    return np.float(Xmin), np.float(Ymin), np.float(Xmax), np.float(Ymax), np.float(axisTiles)






def main():
    
    args=argument_parser().parse_args()
    print('reading tiles from folder: ', args.inputdir)
    print('writing targets to folder: ', args.outputdir)
    print('tiling scheme : ', args.gridextent)
    print('target cell size :', args.targetcellsize)

    minX,minY,maxX,maxY,axisTiles = parse_extent(args.gridextent)

    for filename in os.listdir(args.inputdir):
        if filename.endswith('.LAZ'):

            name = filename.split('.')[0]

            print('Creating target file for '+name)
            createTargetGrid(name,args.outputdir,np.float(args.targetcellsize),minX,minY,maxX,maxY,axisTiles)
     
    
    

if __name__ == "__main__":
    main()
