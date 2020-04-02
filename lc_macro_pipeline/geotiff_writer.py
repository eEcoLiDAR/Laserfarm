#!/usr/bin/env python3

import os
import plyfile
import numpy
import gdal
import time
from osgeo import osr
from lc_macro_pipeline import utils
from lc_macro_pipeline.pipeline import Pipeline
from lc_macro_pipeline.remote_utils import get_wdclient, pull_from_remote, \
    push_to_remote, purge_local

class Geotiff_writer(Pipeline):
    """ Write specified bands from point cloud data into separated geotiff files. """

    def __init__(self):
        self.pipeline = ('localfs',
                         'pullremote',
                         'parse_point_cloud',
                         'data_split',
                         'create_subregion_geotiffs',
                         'pushremote',
                         'cleanlocalfs',
                         )
        self.data_directory = None
        self.output_directory = None
        self.InputTiles = None
        self.subtilelists = []
        self.LengthDataRecord = 0
        self.xResolution = 0
        self.yResolution = 0

    def localfs(self, data_directory, output_directory):
        """
        IO setup for the local file system.

        :param data_directory: full path to input folder on local filesystem.
        :param output_directory: full path to output folder on local filesystem \
                              This folder is considered root for all output \
                              paths specified
        """
        #no check on existance as handled before retrieval
        self.data_directory = data_directory
        check_dir_exists(output_direcotry,should_exist=True,mkdir=True)
        self.output_directory = output_directory
        return self

    def pullremote(self, options, remote_origin):
        """
        pull resource(s) from remote to local fs

        :param options: setup options for webdav client. Can be a filepath
        :param remote_origin: path to resource on remote fs
        """
        wdclient = get_wdclient(options)
        pull_from_remote(wdclient,self.data_directory,remote_origin)
        return self

    def pushremote(self,options,remote_destination):
        """
        push files(s) from local fs to remote fs

        :param options: setup options for the webdav client. Can be filepath
        :param remote_destination: path to remote target directory
        """
        wdclient = get_wdclient(options)
        push_to_remote(wdclient,self.output_directory,remote_destination)
        return self

    def cleanlocalfs(self):
        """
        remove pulled input and results (after push)
        """
        purge_local(self.data_directory)
        purge_local(self.output_directory)
        return self





    def parse_point_cloud(self):
        """
        Parse input point cloud and get the following information:
            - Tile list
            - Length of a single band
            - x and y resolution

        :param data_directory: path to the directory of tiled target point files (.ply)
        """
        self.data_directory = data_directory

        # Get list of input tiles
        utils.check_path_exists(data_directory, should_exist=True)
        self.InputTiles = [TileFile for TileFile in os.listdir(data_directory) if TileFile.endswith('.ply')]


        # Read one tile and get the template
        file=os.path.join(data_directory, self.InputTiles[0])
        template = plyfile.PlyData.read(file)

        # Get length of data record (Nr. of elements in each band)
        self.LengthDataRecord=len(template.elements[0].data)

        # Get resolution, assume a square tile
        self.xResolution = (template.elements[0].data[:]['x'].max() \
                       - template.elements[0].data[:]['x'].min()) \
                       /(numpy.sqrt(template.elements[0].data[:]['x'].size) - 1)
        self.yResolution = (template.elements[0].data[:]['y'].max() \
                       - template.elements[0].data[:]['y'].min()) \
                       /(numpy.sqrt(template.elements[0].data[:]['y'].size) - 1)

        return self


    def data_split(self, xSub, ySub):
        """
        Split the input data into sub-regions

        :param xSub: number of sub-regions in horizontal direction
        :param ySub: number of sub-regions in vertical direction
        """
        xcoord = []
        ycoord = []
        for f in self.InputTiles:
            comp=f.split('_')
            xc=comp[1]
            yc=comp[2].split('.')[0]
            xcoord.append(xc)
            ycoord.append(yc)

        # Tile index list
        xcint = list(map(float,xcoord))
        ycint = list(map(float,ycoord))
        # Extent of the tiles
        maxxc = max(xcint)
        minxc = min(xcint)
        maxyc = max(ycint)
        minyc = min(ycint)
        # Range tile index
        xcRange = maxxc - minxc +1
        ycRange = maxyc - minyc +1
        # Range of each sub-region
        xcSubRange = numpy.floor(xcRange/xSub)
        ycSubRange = numpy.floor(ycRange/ySub)

        # Loop per sub-region, find relevant tile of this new tile
        # Start from bottom left
        for i in range(xSub):
            for j in range(ySub):
                if i != xSub-1 and j!= ySub-1:
                    # Not the last line/colunm
                    # Include left/bottom; Exclude right/up
                    # [Left, Right): [minxc + i*xcSubRange, minxc + (i+1)*xcSubRange);
                    # [Bottom, top): [minyc + j*ycSubRange, minyc + (j+1)*ycSubRange);
                    subtiles = [f for k,f in enumerate(self.InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] < (minxc + (i+1)*xcSubRange) and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] < (minyc + (j+1)*ycSubRange) )]
                if i == xSub-1 and j== ySub-1:
                    # top right corner
                    # [Left, right]: [minxc + i*xcSubRange; maxxc];
                    # [Bottom, top]: [minyc + j*ycSubRange; maxyc];
                    subtiles = [f for k,f in enumerate(self.InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] <= maxxc and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] <= maxyc )]
                if i != xSub-1 and j == ySub-1:
                    # Top line but not top right corner
                    # [Left, right]: [minxc + i*xcSubRange; minxc + (i+1)*xcSubRange];
                    # [Bottom, top]: [minyc + j*ycSubRange; maxyc];
                    subtiles = [f for k,f in enumerate(self.InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] < (minxc + (i+1)*xcSubRange) and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] <= maxyc )]
                if i == xSub-1 and j != ySub-1:
                    # Right colunm but not top right corner
                    # [Left, right]: [minxc + i*xcSubRange; maxxc];
                    # [Bottom, top]: [minyc + j*ycSubRange; minyc + (j+1)*ycSubRange];
                    subtiles = [f for k,f in enumerate(self.InputTiles) if (xcint[k] >= (minxc + i*xcSubRange) and xcint[k] <= maxxc and ycint[k] >= (minyc + j*ycSubRange) and ycint[k] < (minyc + (j+1)*ycSubRange) )]
                self.subtilelists.append(subtiles)
        return self

    def create_subregion_geotiffs(self, outputhandle, band_export, EPSG=28992):
        """
        Export geotiff per sub-region, loop in band dimension

        :param outputhandle: Handle of output file.
                             The output will be named as <outputhandle>_TILE_<tile ID>_BAND_<band name>
        :param band_export: list of features names to export
        :param EPSG: (Optional) EPSG code of the spatial reference system of the input data. Default 28992.
        """
        outfilestem = os.path.join(self.output_directory, outputhandle)
        for subTiffNumber in range(len(self.subtilelists)):
            infiles = self.subtilelists[subTiffNumber]
            print('processing subTiff '+str(subTiffNumber))
            print('      total number of constituent tiles : '+str(len(infiles)))
            if infiles:
                outfile= outfilestem+'_TILE_'+str(subTiffNumber)
                _make_geotiff_per_band(infiles,
                              outfile,
                              band_export,
                              self.data_directory,
                              self.LengthDataRecord,
                              self.xResolution,
                              self.yResolution,
                              EPSG)
            else:
                print('no data in subTiff: '+str(subTiffNumber))
        return self


def _make_geotiff_per_band(infiles,outfile,band_export,data_directory,lengthDataRecord,xResolution,yResolution,EPSG):
    # Set the coordinate frame
    xyData = _plyIntoNumpyArray(data_directory, infiles, lengthDataRecord, ['x', 'y'])
    xyDataShifted = _shiftTerrain(xyData,xResolution,yResolution) # Shift the coordinates to the center of the cell
    geoTransform, arrayinfo = _getGeoTransform(xyDataShifted,xResolution,yResolution)
    indexX, indexY = _getGeoCoding(xyDataShifted,arrayinfo) # GeoCoding: get the index of each point in the raster
    ncols = int(arrayinfo[3])
    nrows = int(arrayinfo[7])

    for band_name in band_export:
        if not band_name in ['x','y']:
            print('Creating GeoTiff for band {!s}...'.format(band_name))
            ct0=time.time()

            # Import one band from PLY
            print('importing data ...')
            terrainDataOneBand = _plyIntoNumpyArray(data_directory, infiles, lengthDataRecord, [band_name])

            # Converet from pointcloud to raster
            RasterData = numpy.full((nrows, ncols), numpy.nan)
            RasterData[indexY, indexX] = terrainDataOneBand[:,0]

            # Write the single band to geotiff
            outfile_band=outfile+"_BAND_"+band_name
            _writeGeoTiff(RasterData,band_name,geoTransform,outfile_band,ncols,nrows,1,EPSG)
            ct1=time.time()
            dct=ct1-ct0
            print('Tiff created in {!s} seconds. Location: {!s}.tif'.format(str(dct), outfile_band))


def _getGeoTransform(xyData, xres, yres):
    '''
        Adpated to accomodate the orientation expected by geotiffs
    '''
    xmin, ymin, xmax, ymax = [xyData[:, 0].min(), xyData[:, 1].min(), xyData[:, 0].max(), xyData[:, 1].max()]
    ncols = round(((xmax - xmin) / xres) +1)
    nrows = round(((ymax - ymin) / yres) +1)
    geotransform = (xmin, xres, 0, ymax, 0, -1.*yres)
    arrayinfo = (xmin,xmax,xres,ncols,ymin,ymax,yres,nrows)
    return geotransform, arrayinfo


def _shiftTerrain(terrainData,xres,yres):
    '''
    This shifts the coordinates by half a cell to account for shift between target list and cell coordinate assumption made by
    gdal accomodating geotiff orientation convention
    '''
    tdc = terrainData.copy()
    tdx = tdc[:,0]
    tdy = tdc[:,1]
    tdx = tdx -0.5*xres
    tdy = tdy -0.5*yres*(-1.)
    tdc[:,0] = tdx
    tdc[:,1] = tdy
    return tdc


def _getGeoCoding(xyData, arrayinfo):
    '''
        Geocoding the point-wise x/y to a raster grid
    '''
    listX = numpy.float32(range(int(arrayinfo[3]))*arrayinfo[2] + arrayinfo[0])
    listY = numpy.float32(range(int(arrayinfo[7]))*arrayinfo[6]*(-1.) + arrayinfo[5])
    dictX = dict(zip(listX, range(len(listX))))
    dictY = dict(zip(listY, range(len(listY))))
    xx = numpy.float32(xyData[:,0])
    yy = numpy.float32(xyData[:,1])
    indexX = [dictX[x] for x in xx]
    indexY = [dictY[y] for y in yy]
    return indexX, indexY


def _writeGeoTiff(featureArrays, bandName, geoTransform, outputFileName, ncols, nrows, nbands, EPSG_code): #TODO: READ EPSG_code FROM INPUT PLY
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


def _plyIntoNumpyArray(directory, tileList, gridLength, columnList):
    terrainData = numpy.empty((gridLength * len(tileList), len(columnList)))
    for i, file in enumerate(tileList):
        if i % 25 == 0 or i == len(tileList)-1 : # first, every 25, and last
            print('processing tile '+str(i+1)+' of '+str(len(tileList)))

        plydata = plyfile.PlyData.read(directory + "/" + file)
        for j, column in enumerate(columnList):
            terrainData[gridLength * i:gridLength * i + gridLength, j] = plydata.elements[0].data[column]
    return terrainData
