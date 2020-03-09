import fire
import pathlib

from laserchicken import build_volume, compute_features, \
    compute_neighborhoods, load, export
import laserchicken.keys
from laserchicken.feature_extractor.feature_extraction import list_feature_names
from laserchicken.io import io_handlers
from laserchicken.normalize import normalize
from laserchicken.utils import create_point_cloud, add_to_point_cloud

from pipeline import Pipeline
from utils import check_path_exists, check_file_exists, check_dir_exists


class DataProcessing(Pipeline):
    """ Read, process and write point cloud data using laserchicken. """

    def __init__(self):
        self.pipeline = ['load', 'normalize', 'extract_features', 'export']
        self.point_cloud = create_point_cloud([], [], [])
        self.targets = create_point_cloud([], [], [])

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

    def normalize(self, cell_size, path=None, **export_opts):
        """
        Normalize point cloud heights.

        :param cell_size: Size of the side of the cell employed for
        normalization (in m)
        :param path: Path where to (optionally) save the normalized point cloud
        :param export_opts: Optional arguments passed to the laserchicken
        export function
        :return:
        """
        normalize(self.point_cloud, cell_size)
        if path is not None:
            export(self.point_cloud, path, **export_opts)
        return self

    def extract_features(self, volume_type, volume_size, targets_path,
                         feature_names, sample_size=None, **load_opts):
        """
        Extract point-cloud features and assign them to the specified target
        point cloud.

        :param volume_type: Type of volume used to construct neighborhoods
        :param volume_size: Size of the volume-related parameter (in m)
        :param targets_path: Path where the target point cloud is located
        :param feature_names: List of the feature names to be computed
        :param sample_size: Sample neighborhoods with a random subset of points
        :param load_opts: Optional arguments passed to the laserchicken load
        function when reading targets
        """
        volume = build_volume(volume_type, volume_size)
        add_to_point_cloud(self.targets, load(targets_path, **load_opts))
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
        Write point-cloud data to disk.

        :param path: Path where to write point-cloud data
        :param attributes: List of attributes to be written in the output file
        :param multi_band_files: If true, write all attributes in one file
        :param export_opts: Optional arguments passed to the laserchicken
        export function
        """
        features = [f for f in self.targets[laserchicken.keys.point].keys()
                    if f not in 'xyz'] if attributes == 'all' else attributes
        for file, feature_set in _get_output_file_dict(path,
                                                       features,
                                                       multi_band_files,
                                                       **export_opts).items():
            export(self.targets, file, attributes=feature_set, **export_opts)
        return self


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


if __name__ == '__main__':
    fire.Fire(DataProcessing)
