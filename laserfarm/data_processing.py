import inspect
import logging
import numpy as np
import pathlib

import laserchicken.keys
from laserchicken import build_volume, compute_features, \
    compute_neighborhoods, load, export, register_new_feature_extractor
from laserchicken.feature_extractor.base_feature_extractor import \
    FeatureExtractor
from laserchicken.feature_extractor.feature_extraction import \
    list_feature_names
from laserchicken.filter import select_above, select_below, select_equal, \
    select_polygon
from laserchicken.io import io_handlers
from laserchicken.kd_tree import initialize_cache
from laserchicken.normalize import normalize
from laserchicken.utils import create_point_cloud, add_to_point_cloud, \
    get_point

from laserfarm.grid import Grid
from laserfarm.pipeline_remote_data import PipelineRemoteData
from laserfarm.utils import check_path_exists, check_file_exists, \
    check_dir_exists, DictToObj

logger = logging.getLogger(__name__)


class DataProcessing(PipelineRemoteData):
    """ Read, process and write point cloud data using laserchicken. """

    def __init__(self, input=None, label=None, tile_index=(None, None)):
        self.pipeline = ('add_custom_feature',
                         'add_custom_features',
                         'load',
                         'normalize',
                         'apply_filter',
                         'export_point_cloud',
                         'generate_targets',
                         'extract_features',
                         'export_targets',
                         'clear_cache')
        self.point_cloud = create_point_cloud([], [], [])
        self.targets = create_point_cloud([], [], [])
        self.grid = Grid()
        self.filter = DictToObj({f.__name__: f
                                 for f in [select_above,
                                           select_below,
                                           select_equal,
                                           select_polygon]})
        self.extractors = DictToObj(_get_extractor_dict())
        self._features = None
        self._tile_index = tile_index
        if input is not None:
            self.input_path = input
        if label is not None:
            self.label = label

    @property
    def features(self):
        self._features = DictToObj(list_feature_names())
        return self._features

    def add_custom_feature(self, extractor_name, **parameters):
        """
        Add customized feature to be computed with laserchicken.

        For information on the available extractors and the corresponding
        parameters:
            $   laserfarm data_processing extractors --help
            $   laserfarm data_processing extractors <extractor_name> --help

        :param extractor_name: Name of the (customizable) extractor
        :param parameters: Extractor-specific parameters
        """
        extractor = _get_attribute(self.extractors, extractor_name)
        _check_parameters_for_extractor(extractor, parameters)
        logger.info('Setting up feature extractor {}'.format(extractor_name))
        register_new_feature_extractor(extractor(**parameters))
        return self

    def add_custom_features(self, custom_feature_list):
        """
        Add a list of customized features to be computed with laserchicken
        (also see add_custom_feature).

        :param custom_feature_list: list of extractor names and parameters
        """
        for custom_feature in custom_feature_list:
            self.add_custom_feature(**custom_feature)
        return self

    def load(self, **load_opts):
        """
        Read point cloud from disk.

        :param load_opts: Arguments passed to the laserchicken load function
        """
        check_path_exists(self.input_path, should_exist=True)
        input_file_list = _get_input_file_list(self.input_path)
        logger.info('Loading point cloud data ...')
        for file in input_file_list:
            logger.info('... loading {}'.format(file))
            add_to_point_cloud(self.point_cloud, load(file, **load_opts))
        logger.info('... loading completed.')
        return self

    def normalize(self, cell_size):
        """
        Normalize point cloud heights.

        :param cell_size: Size of the side of the cell employed for
        normalization (in m)
        :return:
        """
        if not cell_size > 0.:
            raise ValueError('Cell size should be > 0.!')
        _check_point_cloud_is_not_empty(self.point_cloud)
        logger.info('Normalizing point-cloud heights ...')
        normalize(self.point_cloud, cell_size)
        logger.info('... normalization completed.')
        return self

    def apply_filter(self, filter_type, **filter_input):
        """
        Apply a filter to the environment point cloud.

        For information on filter_types and the corresponding input:
            $   laserfarm data_processing filter --help
            $   laserfarm data_processing filter <filter_type> --help

        :param filter_type: Type of filter to apply.
        :param filter_input: Filter-specific input.
        """
        _check_point_cloud_is_not_empty(self.point_cloud)
        filter = _get_attribute(self.filter, filter_type)
        logger.info('Filtering point-cloud data')
        self.point_cloud = filter(self.point_cloud, **filter_input)
        return self

    def export_point_cloud(self, filename='', attributes='all', **export_opts):
        """
        Write environment point cloud to disk.

        :param filename: optional filename where to write point-cloud data
        :param attributes: List of attributes to be written in the output file
        :param export_opts: Optional arguments passed to the laserchicken
                            export function
        """
        expath = self._get_export_path(filename)
        logger.info('Exporting environment point-cloud ...')
        self._export(self.point_cloud,
                     expath,
                     attributes,
                     multi_band_files=True,
                     **export_opts)
        logger.info('... exporting completed.')
        return self

    def generate_targets(self, min_x, min_y, max_x, max_y, n_tiles_side,
                         tile_mesh_size, validate=True,
                         validate_precision=None):
        """
        Generate the target point cloud.

        :param min_x: Min x value of the tiling schema
        :param min_y: Min y value of the tiling schema
        :param max_x: Max x value of the tiling schema
        :param max_y: Max y value of the tiling schema
        :param n_tiles_side: Number of tiles along X and Y (tiling MUST be
        square)
        :param tile_mesh_size: Spacing between target points (in m). The tiles'
        width must be an integer times this spacing
        :param validate: If True, check if all points in the point-cloud belong
        to the same tile
        :param validate_precision: Optional precision threshold to determine
        whether point belong to tile
        """
        logger.info('Setting up the target grid')
        self.grid.setup(min_x, min_y, max_x, max_y, n_tiles_side)

        if any([idx is None for idx in self._tile_index]):
            raise RuntimeError('Tile index not set!')

        if validate:
            logger.info('Checking whether points belong to cell '
                        '({},{})'.format(*self._tile_index))
            x_all, y_all, _ = get_point(self.point_cloud, ...)
            mask = self.grid.is_point_in_tile(x_all,
                                              y_all,
                                              self._tile_index[0],
                                              self._tile_index[1],
                                              validate_precision)
            assert np.all(mask), ('{} points belong to (a) different tile(s)'
                                  '!'.format(len(x_all[~mask])))

        logger.info('Generating target point mesh with '
                    '{}m spacing '.format(tile_mesh_size))
        x_trgts, y_trgts = self.grid.generate_tile_mesh(self._tile_index[0],
                                                        self._tile_index[1],
                                                        tile_mesh_size)
        self.targets = create_point_cloud(x_trgts,
                                          y_trgts,
                                          np.zeros_like(x_trgts))
        return self

    def extract_features(self, volume_type, volume_size, feature_names,
                         sample_size=None):
        """
        Extract point-cloud features and assign them to the specified target
        point cloud.

        :param volume_type: Type of volume used to construct neighborhoods
        :param volume_size: Size of the volume-related parameter (in m)
        :param feature_names: List of the feature names to be computed
        :param sample_size: Sample neighborhoods with a random subset of points
        """
        logger.info('Building volume of type {}'.format(volume_type))
        volume = build_volume(volume_type, volume_size)
        logger.info('Constructing neighborhoods')
        neighborhoods = compute_neighborhoods(self.point_cloud,
                                              self.targets,
                                              volume,
                                              sample_size=sample_size)
        logger.info('Starting feature extraction ...')
        compute_features(self.point_cloud,
                         neighborhoods,
                         self.targets,
                         feature_names,
                         volume)
        logger.info('... feature extraction completed.')
        return self

    def export_targets(self, filename='', attributes='all',
                       multi_band_files=True, **export_opts):
        """
        Write target point cloud to disk.

        :param filename: optional filename where to write point-cloud data
        :param attributes: List of attributes to be written in the output file
        :param multi_band_files: If true, write all attributes in one file
        :param export_opts: Optional arguments passed to the laserchicken
                            export function
        """
        expath = self._get_export_path(filename)
        file_handle = 'tile_{}_{}'.format(*self._tile_index)
        logger.info('Exporting target point-cloud ...')
        self._export(self.targets, expath, attributes, multi_band_files,
                     file_handle, **export_opts)
        logger.info('... exporting completed.')
        return self

    def clear_cache(self):
        """ Clear KDTree's cached by Laserchicken. """
        logger.info('Clearing cached KDTrees ...')
        initialize_cache()
        return self

    @staticmethod
    def _export(point_cloud, path, attributes='all', multi_band_files=True,
                file_handle='point_cloud', **export_opts):
        """
        Write generic point-cloud data to disk.

        :param path: Path where to write point-cloud data
        :param attributes: List of attributes to be written in the output file
        :param multi_band_files: If true, write all attributes in one file
        :param file_handle: Stem for the file name(s) if path is a directory
        :param export_opts: Optional arguments passed to the laserchicken
        export function
        """
        features = [f for f in point_cloud[laserchicken.keys.point].keys()
                    if f not in 'xyz'] if attributes == 'all' else attributes
        for file, feature_set in _get_output_file_dict(path,
                                                       file_handle,
                                                       features,
                                                       multi_band_files,
                                                       **export_opts).items():
            logger.info('... exporting {}'.format(file))
            export(point_cloud, file, attributes=feature_set, **export_opts)

    def _get_export_path(self, filename=''):
        check_dir_exists(self.output_folder, should_exist=True)
        if pathlib.Path(filename).parent.name:
            raise IOError('filename should not include path!')
        return self.output_folder.joinpath(filename).as_posix()


def _check_parameters_for_extractor(extractor, parameters):
    try:
        _ = extractor(**parameters)
    except TypeError:
        raise ValueError('Wrong set of parameters for extractor '
                         '{}'.format(extractor.__name__))


def _get_extractor_dict():
    extractors = {}
    for name, obj in inspect.getmembers(laserchicken.feature_extractor):
        if inspect.ismodule(obj):
            for subname, subobj in inspect.getmembers(obj):
                if (inspect.isclass(subobj)
                        and issubclass(subobj, FeatureExtractor)
                        and subobj is not FeatureExtractor):
                    extractors.update({subname: subobj})
    return extractors


def _get_attribute(obj, attrname):
    attribute = getattr(obj, attrname, None)
    if attribute is None:
        raise ValueError('Invalid attribute: {}. Choose between: '
                         '{}'.format(attrname, ', '.join(obj.__dict__.keys())))
    return attribute


def _get_required_attributes(features=[]):
    attributes = []
    for feature, extractor in list_feature_names().items():
        if feature in features:
            attributes += extractor.requires()
    return attributes


def _get_input_file_list(p):
    check_path_exists(p, should_exist=True)
    if p.is_file():
        files = [str(p.absolute())]
    elif p.is_dir():
        files = sorted([str(f.absolute()) for f in p.iterdir()
                        if f.suffix.lower() in io_handlers.keys()])
        if not files:
            raise FileNotFoundError('No point-cloud file in: {}'.format(p))
    else:
        raise IOError('Unable to read from path: {}'.format(p))
    return files


def _check_point_cloud_is_not_empty(point_cloud):
    pts = point_cloud['vertex']
    if not all([pts[attr]['data'].size > 0 for attr in pts.keys()]):
        raise RuntimeError('Point cloud is empty!')


def _get_output_file_dict(path,
                          file_handle='point_cloud',
                          features=[],
                          multi_band_files=True,
                          format='.ply',
                          overwrite=False,
                          **kwargs):
    p = pathlib.Path(path)
    if not p.suffix:
        # expected dir
        check_dir_exists(p, should_exist=True)
        if features and not multi_band_files:
            files = {}
            for feature in features:
                sub_path = p / feature
                check_dir_exists(sub_path, should_exist=True, mkdir=True)
                file_path = (sub_path / file_handle).with_suffix(format)
                files.update({file_path.as_posix(): [feature]})
        else:
            file_path = (p / file_handle).with_suffix(format).as_posix()
            if features:
                files = {file_path: features}
            else:
                files = {file_path: 'all'}
    else:
        # expected file - check parent dir
        check_dir_exists(p.parent, should_exist=True)
        if features:
            files = {p.as_posix(): features}
        else:
            files = {p.as_posix(): 'all'}

    if not overwrite:
        for file in files.keys():
            check_file_exists(file, should_exist=False)
    return files
