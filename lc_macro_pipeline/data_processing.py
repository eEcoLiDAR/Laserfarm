import inspect
import numpy as np
import pathlib

from laserchicken import build_volume, compute_features, \
    compute_neighborhoods, load, export, register_new_feature_extractor
import laserchicken.keys
from laserchicken.feature_extractor.base_feature_extractor import \
    FeatureExtractor
from laserchicken.feature_extractor.feature_extraction import \
    list_feature_names
from laserchicken.filter import select_above, select_below, select_equal, \
    select_polygon
from laserchicken.io import io_handlers
from laserchicken.normalize import normalize
from laserchicken.utils import create_point_cloud, add_to_point_cloud, \
    get_point

from lc_macro_pipeline.grid import Grid
from lc_macro_pipeline.pipeline import Pipeline
from lc_macro_pipeline.utils import check_path_exists, check_file_exists, \
    check_dir_exists, DictToObj


class DataProcessing(Pipeline):
    """ Read, process and write point cloud data using laserchicken. """

    def __init__(self):
        self.pipeline = ('load',
                         'normalize',
                         'apply_filter',
                         'export_point_cloud',
                         'extract_features',
                         'export_targets')
        self.point_cloud = create_point_cloud([], [], [])
        self.targets = create_point_cloud([], [], [])
        self.grid = Grid()
        self.filter = DictToObj({f.__name__: f
                                 for f in [select_above,
                                           select_below,
                                           select_equal,
                                           select_polygon]})
        self.extractors = DictToObj(_get_extractor_dict())

    @property
    def features(self):
        self._features = DictToObj(list_feature_names())
        return self._features

    def add_custom_feature(self, extractor_name, **parameters):
        """
        Add customized feature to be computed with laserchicken.

        For information on the available extractors and the corresponding
        parameters:
            $   lc_macro_pipeline data_processing extractors --help
            $   lc_macro_pipeline data_processing extractors <extractor_name> --help

        :param extractor_name: Name of the (customizable) extractor
        :param parameters: Extractor-specific parameters
        """
        extractor = _get_attribute(self.extractors, extractor_name)
        register_new_feature_extractor(extractor(**parameters))
        return self

    def load(self, path, **load_opts):
        """
        Read point cloud from disk.

        :param path: Path where to find point cloud data
        :param load_opts: Arguments passed to the laserchicken load function
        """
        for file in _get_input_file_list(path):
            add_to_point_cloud(self.point_cloud,
                               load(file, **load_opts))
        return self

    def normalize(self, cell_size):
        """
        Normalize point cloud heights.

        :param cell_size: Size of the side of the cell employed for
        normalization (in m)
        :return:
        """
        normalize(self.point_cloud, cell_size)
        return self

    def apply_filter(self, filter_type, **filter_input):
        """
        Apply a filter to the environment point cloud.

        For information on filter_types and the corresponding input:
            $   lc_macro_pipeline data_processing filter --help
            $   lc_macro_pipeline data_processing filter <filter_type> --help

        :param filter_type: Type of filter to apply.
        :param filter_input: Filter-specific input.
        """
        filter = _get_attribute(self.filter, filter_type)
        self.point_cloud = filter(self.point_cloud, **filter_input)
        return self

    def export_point_cloud(self, path, attributes='all', **export_opts):
        """
        Write environment point cloud to disk.

        :param path: Path where to write point-cloud data
        :param attributes: List of attributes to be written in the output file
        :param export_opts: Optional arguments passed to the laserchicken
        export function
        """
        self._export(self.point_cloud,
                     path,
                     attributes,
                     multi_band_files=False,
                     **export_opts)
        return self

    def generate_targets(self, min_x, min_y, max_x, max_y, n_tiles_side,
                         index_tile_x, index_tile_y, tile_mesh_size,
                         validate=False):
        """

        :param min_x: Min x value of tiling schema
        :param min_y: Min y value of tiling schema
        :param max_x: Max x value of tiling schema
        :param max_y: Max y value of tiling schema
        :param n_tiles_side: Number of tiles along X and Y (tiling MUST be
        square)
        :param index_tile_x: Index of the tile along X (from 0 to n_tiles_side
        - 1)
        :param index_tile_y: Index of the tile along Y (from 0 to n_tiles_side
        - 1)
        :param tile_mesh_size: Spacing between target points (in m). The tiles'
        width must be an integer times this spacing
        :param validate: If True, check if all points in the point-cloud belong
        to the same tile
        """
        self.grid.setup(min_x, min_y, max_x, max_y, n_tiles_side)

        if validate:
            index_tile = np.array([index_tile_x, index_tile_y], dtype=np.int)
            x_all, y_all, _ = get_point(self.point_cloud, ...)
            index_tile_all = self.grid.get_tile_index(x_all, y_all)
            if not np.all(index_tile_all == index_tile):
                raise Warning('Points belong to different tiles!')

        x_targets, y_targets, plt = self.grid.generate_tile_mesh(index_tile_x,
                                                            index_tile_y,
                                                            tile_mesh_size)
        self.targets = create_point_cloud(x_targets,
                                          y_targets,
                                          np.zeros_like(x_targets))
        return self

    def extract_features(self, volume_type, volume_size, feature_names,
                         sample_size=None, **load_opts):
        """
        Extract point-cloud features and assign them to the specified target
        point cloud.

        :param volume_type: Type of volume used to construct neighborhoods
        :param volume_size: Size of the volume-related parameter (in m)
        :param feature_names: List of the feature names to be computed
        :param sample_size: Sample neighborhoods with a random subset of points
        :param load_opts: Optional arguments passed to the laserchicken load
        function when reading targets
        """
        volume = build_volume(volume_type, volume_size)
        neighborhoods = compute_neighborhoods(self.point_cloud,
                                              self.targets,
                                              volume,
                                              sample_size=sample_size)
        compute_features(self.point_cloud,
                         neighborhoods,
                         self.targets,
                         feature_names,
                         volume)
        return self

    def export_targets(self, path, attributes='all', multi_band_files=True,
                       **export_opts):
        """
        Write target point cloud to disk.

        :param path: Path where to write point-cloud data
        :param attributes: List of attributes to be written in the output file
        :param multi_band_files: If true, write all attributes in one file
        :param export_opts: Optional arguments passed to the laserchicken
        export function
        """
        self._export(self.targets, path, attributes, multi_band_files,
                     **export_opts)
        return self

    @staticmethod
    def _export(point_cloud, path, attributes='all', multi_band_files=True,
                **export_opts):
        """
        Write generic point-cloud data to disk.

        :param path: Path where to write point-cloud data
        :param attributes: List of attributes to be written in the output file
        :param multi_band_files: If true, write all attributes in one file
        :param export_opts: Optional arguments passed to the laserchicken
        export function
        """
        features = [f for f in point_cloud[laserchicken.keys.point].keys()
                    if f not in 'xyz'] if attributes == 'all' else attributes
        for file, feature_set in _get_output_file_dict(path,
                                                       features,
                                                       multi_band_files,
                                                       **export_opts).items():
            export(point_cloud, file, attributes=feature_set, **export_opts)
        return


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


def _get_input_file_list(path):
    p = pathlib.Path(path)
    check_path_exists(p, should_exist=True)
    if p.is_file():
        files = [str(p.absolute())]
    elif p.is_dir():
        files = sorted([str(f.absolute()) for f in p.iterdir()
                        if f.suffix.lstrip('.').lower() in io_handlers.keys()])
    else:
        raise IOError('Unable to read from path: {}'.format(path))
    return files


def _get_output_file_dict(path,
                          features=[],
                          multi_band_files=True,
                          format='ply',
                          overwrite=False,
                          **kwargs):
    p = pathlib.Path(path)
    if p.suffix == '':
        # expected dir
        check_dir_exists(p, should_exist=True)
        if features and not multi_band_files:
            files = {str(p.joinpath('.'.join([feature, format]))): feature
                     for feature in features}
        else:
            files = {str(p.joinpath('.'.join(['all', format]))): 'all'}
    else:
        # expected file - check parent dir
        check_dir_exists(p.parent, should_exist=True)
        if features:
            files = {str(p.absolute()): features}
        else:
            files = {str(p.absolute()): 'all'}

    if not overwrite:
        for file in files.keys():
            check_file_exists(file, should_exist=False)
    return files

