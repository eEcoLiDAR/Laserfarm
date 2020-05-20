import logging
import os
import plyfile
import numpy
import gdal
import time
from osgeo import osr
from laserfarm import utils
from laserfarm.pipeline_remote_data import PipelineRemoteData

logger = logging.getLogger(__name__)


class GeotiffWriter(PipelineRemoteData):
    """ Write specified bands from point cloud data into separate geotiff files. """

    def __init__(self, input_dir=None, bands=None, label=None):
        self.pipeline = ('parse_point_cloud',
                         'data_split',
                         'create_subregion_geotiffs')
        self.InputTiles = []
        self.subtilelists = []
        self.LengthDataRecord = 0
        self.xResolution = 0
        self.yResolution = 0
        if input_dir is not None:
            self.input_path = input_dir
        if bands is not None:
            self.bands = [bands] if isinstance(bands, str) else bands
        if label is not None:
            self.label = label

    def parse_point_cloud(self):
        """
        Parse input point cloud and get the following information:
            - Tile list
            - Length of a single band
            - x and y resolution
        """
        utils.check_dir_exists(self.input_path, should_exist=True)

        # Get list of input tiles
        self.InputTiles = [TileFile
                           for TileFile in os.listdir(self.input_path)
                           if TileFile.lower().endswith('.ply')]
        if not self.InputTiles:
            raise IOError('No PLY file in dir: {}'.format(self.input_path))
        else:
            logger.info('{} PLY files found'.format(len(self.InputTiles)))

        # Read one tile and get the template
        file = os.path.join(self.input_path, self.InputTiles[0])
        template = plyfile.PlyData.read(file)
        if not template.elements[0].name == 'vertex':
            raise ValueError('Tile PLY file should '
                             'have vertex as first object')

        # Get length of data record (Nr. of elements in each band)
        self.LengthDataRecord = len(template.elements[0].data)
        logger.info('No. of points per file: {}'.format(self.LengthDataRecord))

        # Get resolution, assume a square tile
        delta_x = (template.elements[0].data['x'].max()
                   - template.elements[0].data['x'].min())
        delta_y = (template.elements[0].data['y'].max()
                   - template.elements[0].data['y'].min())
        if numpy.isclose(delta_x, 0.) or numpy.isclose(delta_y, 0.):
            raise ValueError('Tile should have finite extend in X and Y!')
        self.xResolution = (delta_x / (numpy.sqrt(template.elements[0].data['x'].size) - 1))
        self.yResolution = (delta_y / (numpy.sqrt(template.elements[0].data['y'].size) - 1))
        if not (numpy.isclose(self.xResolution, self.yResolution) and
                numpy.isclose(delta_x, delta_y)):
            raise ValueError('Tile read is not square!')

        logger.info('Resolution: ({}m x {}m)'.format(self.xResolution,
                                                     self.yResolution))
        return self

    def data_split(self, xSub, ySub):
        """
        Split the input data into sub-regions

        :param xSub: number of sub-regions in horizontal direction
        :param ySub: number of sub-regions in vertical direction
        """
        xcoord = []
        ycoord = []
        if not self.InputTiles:
            raise ValueError('Input tile list is empty!')
        for f in self.InputTiles:
            comp = f.split('_')
            xc = comp[1]
            yc = comp[2].split('.')[0]
            xcoord.append(xc)
            ycoord.append(yc)

        # Tile index list
        xcint = list(map(float, xcoord))
        ycint = list(map(float, ycoord))
        # Extent of the tiles
        maxxc = max(xcint)
        minxc = min(xcint)
        maxyc = max(ycint)
        minyc = min(ycint)
        # Range tile index
        xcRange = maxxc - minxc + 1
        ycRange = maxyc - minyc + 1
        # Range of each sub-region
        xcSubRange = numpy.floor(xcRange / xSub)
        ycSubRange = numpy.floor(ycRange / ySub)

        # Loop per sub-region, find relevant tile of this new tile
        # Start from bottom left
        logger.info('Splitting data into ({}x{}) sub-regions'.format(xSub,
                                                                     ySub))
        for i in range(xSub):
            for j in range(ySub):
                if i != xSub - 1 and j != ySub - 1:
                    # Not the last line/colunm
                    # Include left/bottom; Exclude right/up
                    # [Left, Right): [minxc + i*xcSubRange, minxc + (i+1)*xcSubRange);
                    # [Bottom, top): [minyc + j*ycSubRange, minyc + (j+1)*ycSubRange);
                    subtiles = [f for k, f in enumerate(self.InputTiles)
                                if ((minxc + i * xcSubRange) <= xcint[k] < (minxc + (i + 1) * xcSubRange)
                                and (minyc + j * ycSubRange) <= ycint[k] < (minyc + (j + 1) * ycSubRange))]
                if i == xSub - 1 and j == ySub - 1:
                    # top right corner
                    # [Left, right]: [minxc + i*xcSubRange; maxxc];
                    # [Bottom, top]: [minyc + j*ycSubRange; maxyc];
                    subtiles = [f for k, f in enumerate(self.InputTiles)
                                if ((minxc + i * xcSubRange) <= xcint[k] <= maxxc
                                and (minyc + j * ycSubRange) <= ycint[k] <= maxyc)]
                if i != xSub - 1 and j == ySub - 1:
                    # Top line but not top right corner
                    # [Left, right]: [minxc + i*xcSubRange; minxc + (i+1)*xcSubRange];
                    # [Bottom, top]: [minyc + j*ycSubRange; maxyc];
                    subtiles = [f for k, f in enumerate(self.InputTiles)
                                if ((minxc + i * xcSubRange) <= xcint[k] < (minxc + (i + 1) * xcSubRange)
                                and (minyc + j * ycSubRange) <= ycint[k] <= maxyc)]
                if i == xSub - 1 and j != ySub - 1:
                    # Right colunm but not top right corner
                    # [Left, right]: [minxc + i*xcSubRange; maxxc];
                    # [Bottom, top]: [minyc + j*ycSubRange; minyc + (j+1)*ycSubRange];
                    subtiles = [f for k, f in enumerate(self.InputTiles)
                                if ((minxc + i * xcSubRange) <= xcint[k] <= maxxc
                                and (minyc + j * ycSubRange) <= ycint[k] < (minyc + (j + 1) * ycSubRange))]
                self.subtilelists.append(subtiles)
        return self

    def create_subregion_geotiffs(self, output_handle, EPSG=28992):
        """
        Export geotiff per sub-region, loop in band dimension

        :param output_handle: Handle of output file. The output will be named
        as <output_handle>_TILE_<tile ID>_BAND_<band name>
        :param EPSG: (Optional) EPSG code of the spatial reference system of
        the input data. Default 28992.
        """
        utils.check_dir_exists(self.output_folder, should_exist=True)
        outfilestem = os.path.join(self.output_folder.as_posix(),
                                   output_handle)
        for subTiffNumber in range(len(self.subtilelists)):
            infiles = self.subtilelists[subTiffNumber]
            logger.info('Processing sub-region GeoTiff no. {} '
                        '...'.format(subTiffNumber))
            logger.info('... number of constituent tiles: '
                        '{}'.format(len(infiles)))
            if infiles:
                outfile = '{}_TILE_{:03d}'.format(outfilestem, subTiffNumber)
                _make_geotiff_per_band(infiles,
                                       outfile,
                                       self.bands,
                                       self.input_path.as_posix(),
                                       self.LengthDataRecord,
                                       self.xResolution,
                                       self.yResolution,
                                       EPSG)
            else:
                logger.warning(
                    'No data in sub-region no. ' + str(subTiffNumber))
            logger.info('... processing of sub-region completed.')
        return self


def _make_geotiff_per_band(infiles, outfile, band_export, data_directory,
                           lengthDataRecord, xResolution, yResolution, EPSG):
    # Set the coordinate frame
    logger.debug('... setting the coordinate frame')
    xyData = _plyIntoNumpyArray(data_directory, infiles, lengthDataRecord,
                                ['x', 'y'])
    # Shift the coordinates to the center of the cell
    xyDataShifted = _shiftTerrain(xyData, xResolution, yResolution)
    geoTransform, arrayinfo = _getGeoTransform(xyDataShifted,
                                               xResolution,
                                               yResolution)
    # GeoCoding: get the index of each point in the raster
    indexX, indexY = _getGeoCoding(xyDataShifted, arrayinfo)
    ncols = int(arrayinfo[3])
    nrows = int(arrayinfo[7])

    for band_name in band_export:
        if band_name not in ['x', 'y']:
            logger.debug('... creating GeoTiff for band {!s}'.format(band_name))
            ct0 = time.time()

            # Import one band from PLY
            logger.debug('... importing data')
            terrainDataOneBand = _plyIntoNumpyArray(data_directory,
                                                    infiles,
                                                    lengthDataRecord,
                                                    [band_name])

            # Convert from pointcloud to raster
            RasterData = numpy.full((nrows, ncols), numpy.nan)
            RasterData[indexY, indexX] = terrainDataOneBand[:, 0]

            # Write the single band to geotiff
            outfile_band = outfile + "_BAND_" + band_name
            _writeGeoTiff(RasterData, band_name, geoTransform, outfile_band,
                          ncols, nrows, 1, EPSG)
            ct1 = time.time()
            dct = ct1 - ct0
            logger.debug(('... Tiff created in {!s} seconds. Location: '
                          '{!s}.tif'.format(str(dct), outfile_band)))


def _getGeoTransform(xyData, xres, yres):
    """Adpated to accomodate the orientation expected by geotiffs. """

    xmin, ymin, xmax, ymax = [xyData[:, 0].min(), xyData[:, 1].min(),
                              xyData[:, 0].max(), xyData[:, 1].max()]
    ncols = round(((xmax - xmin) / xres) + 1)
    nrows = round(((ymax - ymin) / yres) + 1)
    geotransform = (xmin, xres, 0, ymax, 0, -1. * yres)
    arrayinfo = (xmin, xmax, xres, ncols, ymin, ymax, yres, nrows)
    return geotransform, arrayinfo


def _shiftTerrain(terrainData, xres, yres):
    """ 
    This shifts the coordinates by half a cell to account for shift between 
    target list and cell coordinate assumption made by gdal accommodating
    geotiff orientation convention.
    """
    tdc = terrainData.copy()
    tdx = tdc[:, 0]
    tdy = tdc[:, 1]
    tdx = tdx - 0.5 * xres
    tdy = tdy - 0.5 * yres * (-1.)
    tdc[:, 0] = tdx
    tdc[:, 1] = tdy
    return tdc


def _getGeoCoding(xyData, arrayinfo):
    """Geocoding the point-wise x/y to a raster grid. """
    x_idx = (xyData[:, 0] - arrayinfo[0]) / arrayinfo[2]
    y_idx = -(xyData[:, 1] - arrayinfo[5]) / arrayinfo[6]
    assert numpy.allclose(x_idx, numpy.rint(x_idx)), 'Geo coding failed!'
    assert numpy.allclose(y_idx, numpy.rint(y_idx)), 'Geo coding failed!'
    return x_idx.astype(int).tolist(), y_idx.astype(int).tolist()


def _writeGeoTiff(featureArrays, bandName, geoTransform, outputFileName, ncols,
                  nrows, nbands, EPSG_code):
    # TODO: READ EPSG_code FROM INPUT PLY
    output_raster = gdal.GetDriverByName('GTiff').Create(
        outputFileName+".tif", ncols, nrows, nbands, gdal.GDT_Float32,
        ['COMPRESS=LZW'])
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
        if i % 25 == 0 or i == len(tileList) - 1:  # first, every 25, and last
            logger.debug('... processing tile ' + str(i+1) + ' of ' + str(len(tileList)))

        plydata = plyfile.PlyData.read(directory + "/" + file)
        for j, column in enumerate(columnList):
            terrainData[gridLength * i:gridLength * i + gridLength, j] = plydata.elements[0].data[column]
    return terrainData
