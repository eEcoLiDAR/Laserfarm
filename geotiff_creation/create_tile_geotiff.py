#!/usr/bin/env python3

"""
script to create a geotiff from ply file
 with extracted features per tile.

"""

import os
import argparse
import plyfile
import numpy
import gdal
import time
from osgeo import osr


"""
argument parser to provide input ArgumentParser
"""
def argument_parser():
    parser=argparse.ArgumentParser()
    parser.add_argument('-dd','--dataDirectory',default=None,help='data directory conntainig ply files with features')
    parser.add_argument('-dl','--dataList',default=None,help='file specifying list of tiles to be used. Optional to full directory')
    parser.add_argument('-lf','--listFlag',default=False,help='boolean flag to specify full directory import (default) or selected tile(s)')
    #parser.add_argument('-xsub','--xSubdivisions',default=1,help='nunmber of x subdivisions')
    #parser.add_argument('-ysub','--ySubdivisions',default=1,help='number of y subdivisions')
    parser.add_argument('-o','--outputDirectory',default='.',help='path to output directory')
    #parser.add_argument('-oh','--outputhandle', default=None,help='file handle for output')

    return parser


"""
combine data into array with raster layers
"""
def combineTerrainFeatures(terrainData,columnNames,arrayInfo):
    nBands = len(columnNames) -3 # remove x,y,z columnNames
    listX = numpy.float32(range(int(arrayInfo['nCols']))*arrayInfo['xResolution'] + arrayInfo['xMin'])
    dictX = dict(zip(listX, range(len(listX))))
    listY = numpy.float32(range(int(arrayInfo['nRows']))*arrayInfo['yResolution']*(-1.) + arrayInfo['yMax'])
    dictY = dict(zip(listY, range(len(listY))))
    arrays = numpy.full((nBands,len(listY),len(listX)),numpy.nan)
    for terrainDatum in terrainData:
        indexX = dictX[numpy.float32(terrainDatum[0])]
        indexY = dictY[numpy.float32(terrainDatum[1])]
        for i in range(nBands):
            arrays[i,indexY,indexX] = terrainDatum[3+i]

    return arrays, nBands


"""
transfer data from ply file into numpy array
"""
def data2NumpyArray(data,lengthDataRecord,columnNames,chosenElement=0):

    terrainData = numpy.empty((lengthDataRecord,len(columnNames)))
    for i, column in enumerate(columnNames):
        terrainData[:,i]=data.elements[chosenElement].data[column]
    return terrainData


"""
get dimensions of data in ply files
"""
def getFileDims(data,chosenElement=0,excludedProperties=['raw_classification','gpstime']):
    lengthDataRecord=len(data.elements[chosenElement].data)
    columnNames=[]

    for i in range(len(data.elements[chosenElement].properties)):
        if data.elements[chosenElement].properties[i].name not in excludedProperties:
            columnNames.append(data.elements[chosenElement].properties[i].name)


    xvals = sorted(numpy.array(list(set(data.elements[chosenElement].data[:]['x']))))
    yvals = sorted(numpy.array(list(set(data.elements[chosenElement].data[:]['y']))))
    xResolution = xvals[1] - xvals[0]
    yResolution = yvals[1] - yvals[0]

    return columnNames, lengthDataRecord, xResolution, yResolution





"""
get geotransform object and info on feature array structure. This has specifed the top left corner.
"""
def getGeoTransform(terrainData, xres, yres):
    xmin, ymin, xmax, ymax = [terrainData[:, 0].min(), terrainData[:, 1].min(), terrainData[:, 0].max(), terrainData[:, 1].max()]
    ncols = round(((xmax - xmin) / xres) +1)
    nrows = round(((ymax - ymin) / yres) +1)
    geoTransform = (xmin, xres, 0, ymax, 0, -1.*yres)
    arrayInfo = dict(xMin=xmin,xMax=xmax,xResolution=xres,nCols=ncols,yMin=ymin,yMax=ymax,yResolution=yres,nRows=nrows)
    return geoTransform, arrayInfo




"""
get (list) of input files. These can be all files in a directory or a list
files (possibly specified by a bounding tuple) located in a directory
"""
def getInputFiles(args):

    if args.listFlag == False:
        InputTiles = getInputFiles_directory(args.dataDirectory)

    else :
        InputTiles = getInputFiles_list(args.dataList, args.dataDirectory)

    return InputTiles


"""
get input files (full directory)
"""
def getInputFiles_directory(Directory):
    if os.path.isdir(Directory) == True:
        InputTiles = [TileFile for TileFile in os.listdir(Directory) if TileFile.endswith('.ply')]
    else:
        InputTiles = []
        print('No valid directory specified')

    return InputTiles



"""
get input files (list)
"""
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




"""
process single input file[i.e. tile] through to geotiff
"""
def processTilePly(args,fn):

    chosenElement=0
    file=os.path.join(args.dataDirectory,fn)
    data= plyfile.PlyData.read(file)

    # declare output file name
    outputFileName = os.path.join(args.outputDirectory,fn.split('.ply')[0])

    columnNames, lengthDataRecord, xResolution, yResolution = getFileDims(data)

    terrainData = data2NumpyArray(data,lengthDataRecord,columnNames)

    terrainDataShifted = shiftTerrainData(terrainData,xResolution,yResolution)

    terrainData2GeoTiff(terrainDataShifted,columnNames,xResolution,yResolution,outputFileName)



"""
Shift the coordinates by half a cell to account for shift between target list and cell coordinate assumption made by gdal
"""
def shiftTerrainData(terrainData,xResolution,yResolution):
    tdc = terrainData.copy()
    tdx = tdc[:,0]
    tdy = tdc[:,1]
    tdx = tdx -0.5*xResolution
    tdy = tdy -0.5*yResolution*(-1.)
    tdc[:,0] = tdx
    tdc[:,1] = tdy
    return tdc



"""
process data in numpy array into raster array format and serialize as geotiff
"""
def terrainData2GeoTiff(terrainDataShifted, columnNames,xResolution,yResolution,outputFileName):
    geoTransform, arrayInfo = getGeoTransform(terrainDataShifted,xResolution,yResolution)
    combinedTerrainFeatures, nBands = combineTerrainFeatures(terrainDataShifted,columnNames,arrayInfo)
    nCols = int(arrayInfo['nCols'])
    nRows = int(arrayInfo['nRows'])
    writeGeoTiff(combinedTerrainFeatures, columnNames, geoTransform,outputFileName,nCols,nRows,nBands)


"""
write geotiff for an input tile
"""
def writeGeoTiff(featureArrays,columnNames,geoTransform,outputFileName,nCols,nRows,nBands):
    output_raster = gdal.GetDriverByName('GTiff').Create(outputFileName + ".tif", nCols, nRows, nBands, gdal.GDT_Float32, ['COMPRESS=LZW'])
    output_raster.SetMetadata(dict(zip(["band_{:02d}_key".format(i) for i in range(1, 1 + nBands)], columnNames[3:])))
    output_raster.SetGeoTransform(geoTransform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(28992)
    output_raster.SetProjection(srs.ExportToWkt())
    for i in range(nBands):
        rb = output_raster.GetRasterBand(1 + i)
        rb.SetMetadata({"band_key": columnNames[3 + i]})
        rb.SetDescription(columnNames[3+i])
        rb.WriteArray(featureArrays[i])
    output_raster.FlushCache()






"""
Main program.
"""
def main():
    args = argument_parser().parse_args()

    #Get list of data to process
    inputTiles = getInputFiles(args)

    if inputTiles != []:
        for tile in inputTiles:
            processTilePly(args,tile)

    else:
        print('No input tiles specified')




"""
Execution if called as main. Otherwise functions are imported
"""

if __name__ == '__main__':
    main()
