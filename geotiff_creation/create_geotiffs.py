#!/usr/bin/env python3

"""
script to create geotiffs from ply files with extracted features.
This can result in VERY large files. If desired and specified a set of geotiffs
of subregions will be created.
"""

import os
import argparse
import plyfile
import numpy
import gdal
import time
from osgeo import osr


def getInputFiles_directory(Directory):
    if os.path.isdir(Directory) == True:
        InputTiles = [TileFile for TileFile in os.listdir(Directory) if TileFile.endswith('.ply')]
    else:
        InputTiles = []
        print('No valid directory specified')

    return InputTiles




def getInputFiles_list(ListFile,SourceDirectory):
    InputTiles = []
    if (os.path.isfile(ListFile) == True and os.path.isdir(SourceDirectory) == True):
        with open(ListFile) as lf:
            for line in lf:
                if line.startswith('#') == False:
                    if line.split('::')[0].strip() == 'tile':
                        tn = line.split('::')[1].strip()+'.ply'
                        if os.path.isfile(SourceDirectory+'/'+tn) == True:
                            InputTiles.append(tn)
                        elif line.split('::')[0].strip() == 'tuple':
                            BoundaryTuple = line.split('::')[1].strip()
                            xmin=int(BoundaryTuple.split(',')[0].strip())
                            xmax=int(BoundaryTuple.split(',')[1].strip())
                            ymin=int(BoundaryTuple.split(',')[2].strip())
                            ymax=int(BoundaryTuple.split(',')[3].strip())
                            for i in range(xmin,xmax+1):
                                for j in range(ymin,ymax+1):
                                    PossibleTile = 'tile_'+str(i)+'_'+str(j)+'.ply'
                                    if os.path.isfile(SourceDirectory+'/'+PossibleTile):
                                        InputTiles.append(PossibleTile)
                        else :
                            print('unclear input format:')
                            print(line)



                else:
                    continue


    else:
        InputTiles=[]
        print('Invalid list and/or directory')


    return InputTiles




def getInputFiles(args):

    if args.listFlag == False:
        InputTiles = getInputFiles_directory(args.dataDirectory)

    else :
        InputTiles = getInputFiles_list(args.dataList, args.dataDirectory)

    return InputTiles





def getFileTemplate(args,fn):

    file=os.path.join(args.dataDirectory,fn)
    template = plyfile.PlyData.read(file)
    ChosenElement=0

    FileSize = os.path.getsize(file)
    LengthDataRecord=len(template.elements[ChosenElement].data)
    ColumnNames=[]

    for i in range(len(template.elements[ChosenElement].properties)):
        if template.elements[ChosenElement].properties[i].name != 'raw_classification':
            ColumnNames.append(template.elements[ChosenElement].properties[i].name)


    xvals = sorted(numpy.array(list(set(template.elements[ChosenElement].data[:]['x']))))
    yvals = sorted(numpy.array(list(set(template.elements[ChosenElement].data[:]['y']))))
    xResolution = xvals[1] - xvals[0]
    yResolution = yvals[1] - yvals[0]

    return ColumnNames, LengthDataRecord, xResolution, yResolution




def dataSplit(InputTiles, xSub, ySub):
    subtilelists = []
    xcoord = []
    ycoord = []
    for f in InputTiles:
        comp=f.split('_')
        xc=comp[1]
        yc=comp[2].split('.')[0]
        xcoord.append(xc)
        ycoord.append(yc)

    xcint = list(map(float,xcoord))
    ycint = list(map(float,ycoord))
    maxxc = max(xcint)
    #print(maxxc)
    minxc = min(xcint)
    #print(minxc)
    maxyc = max(ycint)
    #print(maxyc)
    minyc = min(ycint)
    #print(minyc)
    xcRange = maxxc - minxc +1
    #print(xcRange)
    ycRange = maxyc - minyc +1
    #print(ycRange)
    xcSubRange = numpy.floor(xcRange/xSub)
    #print(xcSubRange)
    xcExcess = numpy.mod(xcRange,xSub)
    #print(xcExcess)
    ycSubRange = numpy.floor(ycRange/ySub)
    #print(ycSubRange)
    ycExcess = numpy.mod(ycRange,ySub)
    #print(ycExcess)

    for i in range(xSub):
        for j in range(ySub):
            #print('i:'+str(i)+'j:'+str(j))
            if i != xSub-1 and j!= ySub-1:
                subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] < (minxc + (i+1)*xcSubRange) and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] < (minyc + (j+1)*ycSubRange) )]
            if i == xSub-1 and j== ySub-1:
                subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] <= maxxc and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] <= maxyc )]
                #subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] in range(minxc + i*xcSubRange, maxxc +1) and ycint[k] in range(minyc + i*ycSubRange, maxyc + 1) )]
            if i != xSub-1 and j == ySub-1:
                subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] < (minxc + (i+1)*xcSubRange) and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] <= maxyc )]

#subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] in range(minxc + i*xcSubRange, minxc + (i+1)*xcSubRange) and ycint[k] in range(minyc + i*ycSubRange, maxyc + 1) )]
            if i == xSub-1 and j != ySub-1:
                subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] <= maxxc and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] < (minyc + (j+1)*ycSubRange) )]
                #subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] in range(minxc + i*xcSubRange, maxxc + 1) and ycint[k] in range(minyc + i*ycSubRange, minyc + (i+1)*ycSubRange) )]

            subtilelists.append(subtiles)

    return subtilelists



def plyIntoNumpyArray(directory, tileList, gridLength, columnList):
    terrainData = numpy.empty((gridLength * len(tileList), len(columnList)))
    for i, file in enumerate(tileList):
        if i % 25 == 0 :
            print('processing tile '+str(i+1)+' of '+str(len(tileList)))

        plydata = plyfile.PlyData.read(directory + "/" + file)
        for j, column in enumerate(columnList):
            terrainData[gridLength * i:gridLength * i + gridLength, j] = plydata.elements[0].data[column]
    return terrainData



"""
This shifts the coordinates by half a cell to account for shift between target list and cell coordinate assumption made by gdal, accomodating geotiff orientation convention
"""
def shiftTerrain(terrainData,xres,yres):
    tdc = terrainData.copy()
    tdx = tdc[:,0]
    tdy = tdc[:,1]
    tdx = tdx -0.5*xres
    tdy = tdy -0.5*yres*(-1.)
    tdc[:,0] = tdx
    tdc[:,1] = tdy
    return tdc

"""
adpated to accomodate the orientation expected by geotiffs
"""
def getGeoTransform(terrainData, xres, yres):
    xmin, ymin, xmax, ymax = [terrainData[:, 0].min(), terrainData[:, 1].min(), terrainData[:, 0].max(), terrainData[:, 1].max()]
    ncols = round(((xmax - xmin) / xres) +1)
    nrows = round(((ymax - ymin) / yres) +1)
    geotransform = (xmin, xres, 0, ymax, 0, -1.*yres)
    arrayinfo = (xmin,xmax,xres,ncols,ymin,ymax,yres,nrows)
    return geotransform, arrayinfo


def combineTerrainFeatures(terrainData, terrainHeader, arrayinfo ):
    bands = len(terrainHeader) - 3 # removing x, y and z
    #listX = numpy.unique(terrainData[:, 0])
    listX = numpy.float32(range(int(arrayinfo[3]))*arrayinfo[2] + arrayinfo[0])
    dictX = dict(zip(listX, range(len(listX))))
    #listY = numpy.unique(terrainData[:, 1])
    listY = numpy.float32(range(int(arrayinfo[7]))*arrayinfo[6]*(-1.) + arrayinfo[5])
    dictY = dict(zip(listY, range(len(listY))))
    arrays = numpy.full((bands, len(listY), len(listX)), numpy.nan)
    for terrainDatum in terrainData:
        indexX = dictX[numpy.float32(terrainDatum[0])]
        indexY = dictY[numpy.float32(terrainDatum[1])]
        for i in range(bands):
            arrays[i, indexY, indexX] = terrainDatum[3 + i]

    return arrays, bands #, len(listY), len(listX)


def writeGeoTiff(featureArrays, terrainHeader, geoTransform, outputFileName, ncols, nrows, bands):
    output_raster = gdal.GetDriverByName('GTiff').Create(outputFileName + ".tif", ncols, nrows, bands, gdal.GDT_Float32, ['COMPRESS=LZW'])
    output_raster.SetMetadata(dict(zip(["band_{:02d}_key".format(i) for i in range(1, 1 + bands)], terrainHeader[3:])))
    #output_raster.SetMetadata("AREA_OR_POINT=POINT")
    output_raster.SetGeoTransform(geoTransform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(28992)
    output_raster.SetProjection(srs.ExportToWkt())
    for i in range(bands):
        rb = output_raster.GetRasterBand(1 + i)
        rb.SetMetadata({"band_key": terrainHeader[3 + i]})
        rb.WriteArray(featureArrays[i])
    output_raster.FlushCache()




def terrainDataToGeoTiff(terrainData, terrainHeader, xres, yres, outputFileName):
    geoTransform, arrayinfo = getGeoTransform(terrainData,xres,yres)
    combinedTerrainFeatures, bands = combineTerrainFeatures(terrainData,terrainHeader,arrayinfo)
    ncols = int(arrayinfo[3])
    nrows = int(arrayinfo[7])
    writeGeoTiff(combinedTerrainFeatures, terrainHeader,geoTransform,outputFileName,ncols,nrows,bands)




def make_geotiff(args,infiles,lengthDataRecord,terrainHeader,xResolution, yResolution,outfile):
    print('importing data ...')
    it0=time.time()
    terrainData = plyIntoNumpyArray(args.dataDirectory, infiles, lengthDataRecord, terrainHeader)
    it1=time.time()
    dit = it1 - it0
    print('imported in '+str(dit)+' seconds')
    st0=time.time()
    terrainDataShifted = shiftTerrain(terrainData,xResolution,yResolution)
    st1=time.time()
    dst=st1-st0
    print('Coordinate shift calculated in '+str(dst)+' seconds')
    print('Creating GeoTiff ...')
    ct0=time.time()
    terrainDataToGeoTiff(terrainDataShifted,terrainHeader,xResolution,yResolution,outfile)
    ct1=time.time()
    dct=ct1-ct0
    print('(sub)GeoTiff written in '+str(dct)+' seconds')





def create_subregion_geotiffs(args, subTileLists, terrainHeader, xresolution, yresolution,lengthDataRecord):
    outfilestem = os.path.join(args.outputdir,args.outputhandle)
    for subTiffNumber in range(len(subTileLists)):
        infiles = subTileLists[subTiffNumber]
        print('processing subTiff '+str(subTiffNumber))
        print('      total number of constituent tiles : '+str(len(infiles)))


        if infiles != []:
            outfile= args.outputdir+'/'+args.outputhandle+'_'+str(subTiffNumber)
            make_geotiff(args,infiles,lengthDataRecord,terrainHeader,xresolution,yresolution,outfile)

        else:
            print('no data in subTiff: '+str(subTiffNumber))



def argument_parser():
    parser=argparse.ArgumentParser()
    parser.add_argument('-dd','--dataDirectory',default=None,help='data directory conntainig ply files with features')
    parser.add_argument('-dl','--dataList',default=None,help='file specifying list of tiles to be used. Optional to full directory')
    parser.add_argument('-lf','--listFlag',default=False,help='boolean flag to specify full directory import (default) or selected tile(s)')
    parser.add_argument('-xsub','--xSubdivisions',default=1,help='nunmber of x subdivisions')
    parser.add_argument('-ysub','--ySubdivisions',default=1,help='number of y subdivisions')
    parser.add_argument('-o','--outputdir',default='.',help='path to output directory')
    parser.add_argument('-oh','--outputhandle', default=None,help='file handle for output')

    return parser

def main():

    args = argument_parser().parse_args()

    #Get list of data to process
    InputTiles = getInputFiles(args)

    #Get template of datafiles
    terrainHeader, lengthDataRecord, xResolution, yResolution = getFileTemplate(args,InputTiles[0])

    subTileLists = dataSplit(InputTiles,numpy.int(args.xSubdivisions),numpy.int(args.ySubdivisions))

    create_subregion_geotiffs(args, subTileLists, terrainHeader, xResolution, yResolution,lengthDataRecord)



if __name__ == '__main__':
    main()
