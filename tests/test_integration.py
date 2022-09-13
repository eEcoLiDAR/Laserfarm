import json
import os
import shutil

from laserfarm.data_processing import DataProcessing
from laserfarm.geotiff_writer import GeotiffWriter
from laserfarm.retiler import Retiler
from .tools import TestDerivedRemoteDataPipeline, write_PLY_targets, \
    get_number_of_points_in_LAZ_file


class TestRetiler(TestDerivedRemoteDataPipeline):

    _test_dir = 'test_tmp_dir'
    _input_file = 'C_43FN1_1.LAZ'
    _log_filename = 'retiler.log'

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = Retiler()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    @property
    def input(self):
        _input = {
            'log_config': {
                'filename': self._log_filename
            },
            'setup_local_fs': {
                'input_folder': 'testdata',
                'output_folder': self._test_dir
            },
            'set_grid': {
                'min_x': -113107.8100,
                'max_x': 398892.1900,
                'min_y': 214783.8700,
                'max_y': 726783.87,
                'n_tiles_side': 256
            },
            'split_and_redistribute': {},
            'validate': {}
        }
        return _input

    def test_FullPipeline(self):
        self.pipeline.input_path = self._input_file
        self.pipeline.input = self.input
        self.pipeline.run()

        # log file is present
        self.assertTrue(os.path.join(self._test_dir, self._log_filename))

        # retile record file is present
        json_filename = _get_retile_record_filename(self._input_file)
        filepath = os.path.join(self._test_dir, json_filename)
        self.assertTrue(os.path.isfile(filepath))

        # content of the retile record file is correct
        with open(filepath, 'r') as f:
            results = json.load(f)
        self.assertTrue("tile_101_102" in results['redistributed_to'])
        self.assertTrue("tile_101_101" in results['redistributed_to'])
        self.assertEqual(len(results['redistributed_to']), 2)
        self.assertTrue(results['validated'])

        # retiled files exist and contain correct number of points
        for filename, expected_points in zip(("tile_101_101/C_43FN1_1_1.LAZ",
                                              "tile_101_102/C_43FN1_1_2.LAZ"),
                                             (307670, 1210)):
            filepath = os.path.join(self._test_dir, filename)
            self.assertTrue(os.path.isfile(filepath))
            self.assertEqual(get_number_of_points_in_LAZ_file(filepath),
                             expected_points)


def _get_retile_record_filename(input_filename):
    stem_input_file = os.path.splitext(input_filename)[0]
    return "_".join([stem_input_file, "retile_record.js"])


class TestDataProcessing(TestDerivedRemoteDataPipeline):

    _test_dir = 'test_tmp_dir'
    _input_files = ['C_43FN1_1_1.LAZ', 'C_43FN1_1_2.LAZ']
    _log_filename = 'data_processing.log'
    _output_point_cloud = 'point_cloud.laz'
    _tile_index = (101, 101)
    _features = ['point_density', 'band_ratio_z_0.0']

    def setUp(self):
        os.mkdir(self._test_dir)
        for input_file in self._input_files:
            src = os.path.join('testdata', input_file)
            shutil.copy(src, self._test_dir)
        self.pipeline = DataProcessing()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    @property
    def input(self):
        _input = {
            'log_config': {
                'filename': self._log_filename
            },
            'setup_local_fs': {
                'input_folder': self._test_dir,
                'output_folder': self._test_dir,
            },
            'load': {
                'attributes': ['intensity']
            },
            'normalize': {
                'cell_size': 10.,
            },
            'apply_filter': {
                'filter_type': 'select_below',
                'attribute': 'x',
                'threshold': 90050.
            },
            'export_point_cloud': {
                'filename': self._output_point_cloud,
            },
            'generate_targets': {
                'min_x': -113108.,
                'max_x': 398892.,
                'min_y': 214784.,
                'max_y': 726784.,
                'n_tiles_side': 256,
                'tile_mesh_size': 10.,
                'validate': True,
            },
            'extract_features': {
                'volume_type': 'cell',
                'volume_size': 10.,
                'feature_names': self._features,
            },
            'export_targets': {
                'attributes': self._features,
                'multi_band_files': False
            }
        }
        return _input

    def test_FullPipeline(self):
        self.pipeline.add_custom_feature('BandRatioFeatureExtractor',
                                         lower_limit=None,
                                         upper_limit=0.,
                                         data_key='z')
        self.pipeline._tile_index = self._tile_index
        self.pipeline.input = self.input
        self.pipeline.run()

        # log file is present
        self.assertTrue(os.path.join(self._test_dir, self._log_filename))

        # env point cloud is present
        filepath = os.path.join(self._test_dir, self._output_point_cloud)
        self.assertTrue(os.path.isfile(filepath))

        # check number of points
        self.assertEqual(get_number_of_points_in_LAZ_file(filepath),
                         14422)

        for feature in self._features:
            # feature-specific target files are present
            filename = '{}/tile_{}_{}.ply'.format(feature.replace('<', ''),
                                                  self._tile_index[0],
                                                  self._tile_index[1])
            filepath = os.path.join(self._test_dir, filename)
            self.assertTrue(os.path.isfile(filepath))

            with open(filepath, 'r') as f:
                line = f.readline()
                while not line.startswith('end_header'):
                    # they contain the right attributes
                    if line.startswith('property'):
                        self.assertIn(line.split()[2], ['x', 'y', 'z', feature])
                    # they contain the right number of points
                    if line.startswith('element vertex'):
                        self.assertEqual(int(line.split()[-1]), 40000)
                    line = f.readline()


class TestGeotiffWriter(TestDerivedRemoteDataPipeline):

    _test_dir = 'test_tmp_dir'
    _log_filename = 'geotiff_writer.log'
    _handle = 'geotiff'
    _features = ["z", "feature_1", "feature_2"]
    _n_subregions = (2, 2)

    def setUp(self):
        os.mkdir(self._test_dir)
        indices = [(nx, ny) for nx in [10, 11] for ny in [12, 13]]
        write_PLY_targets(self._test_dir, indices=indices)
        self.pipeline = GeotiffWriter()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    @property
    def input(self):
        _input = {
            'log_config': {
                'filename': self._log_filename
            },
            'setup_local_fs': {
                'input_folder': self._test_dir,
                'output_folder': self._test_dir,
            },
            "parse_point_cloud": {},
            "data_split": {"xSub": self._n_subregions[0],
                           "ySub": self._n_subregions[1]},
            "create_subregion_geotiffs": {
                "output_handle": self._handle,
            }
        }
        return _input

    def test_FullPipeline(self):
        self.pipeline.bands = self._features
        self.pipeline.input = self.input
        self.pipeline.run()

        # log file is present
        self.assertTrue(os.path.join(self._test_dir, self._log_filename))

        # geotiff files are present
        index = 0
        for nx in range(self._n_subregions[0]):
            for ny in range(self._n_subregions[1]):
                for feature in self._features:
                    file_name = '{}_TILE_{:03d}_BAND_{}.tif'.format(self._handle,
                                                                    index,
                                                                    feature)
                    file_path = os.path.join(self._test_dir, file_name)
                    print(file_path)
                    self.assertTrue(os.path.isfile(file_path))
                index += 1


