import fire
import pathlib

from laserchicken import build_volume, compute_features, \
    compute_neighborhoods, load, export
import laserchicken.keys
from laserchicken.feature_extractor.feature_extraction import list_feature_names
from laserchicken.io import io_handlers
from laserchicken.normalize import normalize
from laserchicken.utils import create_point_cloud, add_to_point_cloud

from utils import check_path_exists, get_args_from_configfile


class DataProcessing(object):
    """ Read, process and write point cloud data. """

    _PIPELINE = ['load', 'normalize', 'extract_features', 'export']

    def __init__(self):
        self.point_cloud = create_point_cloud([], [], [])
        self.targets = None

    def load(self, path, **load_opts):
        """
        Read point cloud from disk

        :param path: path where to find point cloud data
        :param load_opts: arguments passed to laserchicken load function
        """
        for file in _get_input_file_list(path):
            add_to_point_cloud(self.point_cloud,
                               load(file, **load_opts))
        return self

    def normalize(self, cell_size):
        """
        Normalize point cloud heights

        :param cell_size:
        :return:
        """
        normalize(self.point_cloud, cell_size)
        return self

    def extract_features(self, volume_type, volume_size, targets_path,
                         feature_names, sample_size=None):
        """
        Extract point-cloud features and assign them to a provided target point
        cloud.

        :param volume_type: type of volume used to construct neighborhoods
        :param volume_size: size of the volume-related parameter (in m)
        :param targets_path: path where the target point cloud is located
        :param feature_names: list of the feature names to be computed
        :param sample_size: sample
        """
        volume = build_volume(volume_type, volume_size)
        self.targets = load(targets_path)
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

    def export(self, path, attributes='all', multi_band_files=True,
               **export_opts):
        """
        Write point cloud to disk.

        :param path: path where to write point-cloud data
        :param attributes: list of attributes to be written in the output file
        :param multi_band_files: if true, write all attributes in one file
        :param export_opts: optional arguments passed to the laserchicken
        export function
        """
        if self.targets is None:
            point_cloud = self.point_cloud
        else:
            point_cloud = self.targets
        features = [f for f in point_cloud[laserchicken.keys.point].keys()
                    if f not in 'xyz'] if attributes == 'all' else attributes
        for file, feature_set in _get_output_file_dict(path,
                                                       features,
                                                       multi_band_files,
                                                       **export_opts).items():
            export(point_cloud, file, attributes=feature_set, **export_opts)
        return self

    def run_pipeline(self, path):
        """
        Run full data-processing pipeline using input from configfile.
        Only the tasks for which attributes are present in the file will be
        performed.

        :param path: path where the configfile is located
        """
        args = get_args_from_configfile(path)
        for task_name in self._PIPELINE:
            if task_name in args:
                task = getattr(self, task_name)
                self = task(**args[task_name])
        return self

    def __str__(self):
        return 'Done!'


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
        check_path_exists(p, should_exist=True)
        if features and not multi_band_files:
            files = {str(p.joinpath('.'.join([feature, format]))): feature
                         for feature in features}
        else:
            files = {str(p.joinpath('.'.join(['all', format]))): 'all'}
    else:
        # expected file
        check_path_exists(p.parent, should_exist=True)
        if features:
            files = {str(p.absolute()): features}
        else:
            files = {str(p.absolute()): 'all'}

    if not overwrite:
        for file in files.keys():
            check_path_exists(file, should_exist=False)
    return files


if __name__ == '__main__':
    fire.Fire(DataProcessing)
