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
    if args.dataList is None:
        InputTiles = getInputFiles_directory(args.dataDirectory)
    else:
        InputTiles = getInputFiles_list(args.dataList, args.dataDirectory)

    return InputTiles





def getFileTemplate(args,fn):

    file=os.path.join(args.dataDirectory,fn)
    template = plyfile.PlyData.read(file)
    ChosenElement=0

    # FileSize = os.path.getsize(file)
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
    minxc = min(xcint)
    maxyc = max(ycint)
    minyc = min(ycint)
    xcRange = maxxc - minxc +1
    ycRange = maxyc - minyc +1
    xcSubRange = numpy.floor(xcRange/xSub)
    # xcExcess = numpy.mod(xcRange,xSub)
    ycSubRange = numpy.floor(ycRange/ySub)
    # ycExcess = numpy.mod(ycRange,ySub)


    for i in range(xSub):
        for j in range(ySub):
            if i != xSub-1 and j!= ySub-1:
                subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] < (minxc + (i+1)*xcSubRange) and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] < (minyc + (j+1)*ycSubRange) )]
            if i == xSub-1 and j== ySub-1:
                subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] <= maxxc and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] <= maxyc )]
            if i != xSub-1 and j == ySub-1:
                subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] < (minxc + (i+1)*xcSubRange) and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] <= maxyc )]
            if i == xSub-1 and j != ySub-1:
                subtiles = [f for k,f in enumerate(InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] <= maxxc and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] < (minyc + (j+1)*ycSubRange) )]
            subtilelists.append(subtiles)

    return subtilelists



def plyIntoNumpyArray(directory, tileList, gridLength, columnList):
    terrainData = numpy.empty((gridLength * len(tileList), len(columnList)))
    for i, file in enumerate(tileList):
        if i % 25 == 0 or i == len(tileList)-1 : # first, every 25, and last
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
def getGeoTransform(xyData, xres, yres):
    xmin, ymin, xmax, ymax = [xyData[:, 0].min(), xyData[:, 1].min(), xyData[:, 0].max(), xyData[:, 1].max()]
    ncols = round(((xmax - xmin) / xres) +1)
    nrows = round(((ymax - ymin) / yres) +1)
    geotransform = (xmin, xres, 0, ymax, 0, -1.*yres)
    arrayinfo = (xmin,xmax,xres,ncols,ymin,ymax,yres,nrows)
    return geotransform, arrayinfo

"""
from point wise data to pixel da
"""

def getGeoCoding(xyData, arrayinfo):
    listX = numpy.float32(range(int(arrayinfo[3]))*arrayinfo[2] + arrayinfo[0])
    listY = numpy.float32(range(int(arrayinfo[7]))*arrayinfo[6]*(-1.) + arrayinfo[5])
    dictX = dict(zip(listX, range(len(listX))))
    dictY = dict(zip(listY, range(len(listY))))
    xx = numpy.float32(xyData[:,0])
    yy = numpy.float32(xyData[:,1])
    indexX = [dictX[x] for x in xx]
    indexY = [dictY[y] for y in yy]
    return indexX, indexY


def writeGeoTiff(featureArrays, bandName, geoTransform, outputFileName, ncols, nrows, nbands, EPSG_code=28992): #TODO: READ EPSG_code FROM INPUT PLY
    output_raster = gdal.GetDriverByName('GTiff').Create(outputFileName + ".tif", ncols, nrows, nbands, gdal.GDT_Float32, ['COMPRESS=LZW'])
    output_raster.SetGeoTransform(geoTransform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(EPSG_code)
    output_raster.SetProjection(srs.ExportToWkt())
    output_raster.SetMetadata({'band': bandName})
    rb = output_raster.GetRasterBand(1)
    rb.SetMetadata({"band_key": bandName})
    rb.WriteArray(featureArrays)
    output_raster.FlushCache()


def make_geotiff(args,infiles,lengthDataRecord,terrainHeader,xResolution,yResolution,outfile):
    # Set the coordinate frame
    xyData = plyIntoNumpyArray(args.dataDirectory, infiles, lengthDataRecord, ['x', 'y'])
    xyDataShifted = shiftTerrain(xyData,xResolution,yResolution) # Shift the coordinates to the center of the cell
    geoTransform, arrayinfo = getGeoTransform(xyDataShifted,xResolution,yResolution)
    indexX, indexY = getGeoCoding(xyDataShifted,arrayinfo) # GeoCoding: get the index of each point in the raster
    ncols = int(arrayinfo[3])
    nrows = int(arrayinfo[7])

    for band_name in terrainHeader:
        if not band_name in ['x','y']:
            print('Creating GeoTiff for band {!s}...'.format(band_name))
            ct0=time.time()

            # Import one band from PLY
            print('importing data ...')
            terrainDataOneBand = plyIntoNumpyArray(args.dataDirectory, infiles, lengthDataRecord, [band_name])
            
            # Converet from pointcloud to raster
            RasterData = numpy.full((nrows, ncols), numpy.nan)
            RasterData[indexY, indexX] = terrainDataOneBand[:,0]

            # Write the single band to geotiff
            outfile_band=outfile+"_BAND_"+band_name
            writeGeoTiff(RasterData,band_name,geoTransform,outfile_band,ncols,nrows,1)
            ct1=time.time()
            dct=ct1-ct0
            print('Tiff created in {!s} seconds. Location: {!s}.tif'.format(str(dct), outfile_band))



def create_subregion_geotiffs(args, subTileLists, terrainHeader, xresolution, yresolution,lengthDataRecord):
    outfilestem = os.path.join(args.outputdir,args.outputhandle)

    for subTiffNumber in range(len(subTileLists)):
        infiles = subTileLists[subTiffNumber]
        print('processing subTiff '+str(subTiffNumber))
        print('      total number of constituent tiles : '+str(len(infiles)))

        if infiles != []:
            outfile= outfilestem+'_TILE_'+str(subTiffNumber)
            make_geotiff(args,infiles,lengthDataRecord,terrainHeader,xresolution,yresolution,outfile)
        else:
            print('no data in subTiff: '+str(subTiffNumber))



def parse_argument():
    parser=argparse.ArgumentParser(description='Export geotiff files from target points PLY files.')
    requiredArg = parser.add_argument_group('required arguments')
    optionalArg = parser.add_argument_group('optional arguments')
    requiredArg.add_argument('-dd','--dataDirectory',default=None,help='data directory conntainig ply files with features')
    requiredArg.add_argument('-f','--featureList', default=None,help='list of features to export. Default is all features.')
    optionalArg.add_argument('-o','--outputdir',default='./GeotiffOutput',help='path to output directory. Default is "./GeotiffOutput/".')
    optionalArg.add_argument('-oh','--outputhandle', default='Geotiff',help='the output will be named as <outputhandle>_TILE_<tile ID>_BAND_<band name>. Default "Geotiff".')
    optionalArg.add_argument('-dl','--dataList',default=None,help='file specifying list of tiles in  to be used. By default the full directory will be used.')
    optionalArg.add_argument('-xsub','--xSubdivisions',default=1,help='nunmber of x subdivisions. Default: 1')
    optionalArg.add_argument('-ysub','--ySubdivisions',default=1,help='number of y subdivisions. Default: 1') 
    args = parser.parse_args()
    # Check two required args: dataDirectory and featureList
    if (args.dataDirectory is None):
        print('Missing input argument "DATADIRECTORY".')
        parser.print_usage() 
        exit()
    elif (args.featureList is None):
        print('Missing input argument "FEATURELIST".')
        parser.print_usage()
        exit()
    return args

def main():
    # Args loading
    args = parse_argument()

    # Data loading
    InputTiles = getInputFiles(args) # Get list of data to process
    terrainHeader, lengthDataRecord, xResolution, yResolution = getFileTemplate(args,InputTiles[0])  # Get template of datafiles from the first file

    # Input data sanitory check
    assert ('x' in terrainHeader), "no x coordinates found!"
    assert ('y' in terrainHeader), "no y coordinates found!"
    assert (len(terrainHeader)>2), "no feature found other than x and y!"
    
    
    # Check if the features of interest exist in data
    if args.featureList is not None:
        fList = ['x', 'y'] # Fetaures which will always be included
        fListIn = args.featureList.split(',')
        for ft in fListIn:
            if ft in terrainHeader:
                fList.append(ft)
            else: 
                print('Skipping feature "{!s}" because it does not exist in the input data.'.format(ft))
        terrainHeader = fList
    assert (len(terrainHeader)>2), "All feature specified are not in the input data!"

    # Re-tile
    subTileLists = dataSplit(InputTiles,numpy.int(args.xSubdivisions),numpy.int(args.ySubdivisions))

    # Make geotiffs for each retiles sub-region
    create_subregion_geotiffs(args, subTileLists, terrainHeader, xResolution, yResolution,lengthDataRecord)
 


if __name__ == '__main__':
    main()
