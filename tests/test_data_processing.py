import os
import pathlib
import shutil
import unittest

import numpy as np

from laserfarm.data_processing import DataProcessing
from .tools import create_test_point_cloud, get_number_of_points_in_LAZ_file


class TestInitializeDataProcessing(unittest.TestCase):

    def test_initDefault(self):
        dp = DataProcessing()
        self.assertIsInstance(dp.input_path, pathlib.Path)
        self.assertEqual(
            dp.input_path.absolute().as_posix(),
            pathlib.Path.cwd().as_posix()
        )

    def test_initRelativePath(self):
        filepath = 'dir/file.dat'
        dp = DataProcessing(input=filepath)
        self.assertEqual(
            dp.input_path.absolute().as_posix(),
            (pathlib.Path.cwd() / filepath).as_posix()
        )

    def test_initAbsolutePath(self):
        filepath = pathlib.Path('/dir/file.dat')
        dp = DataProcessing(input=filepath)
        self.assertEqual(
            dp.input_path.absolute().as_posix(),
            filepath.absolute().as_posix()
        )


class TestAddCustomFeature(unittest.TestCase):

    def setUp(self):
        self.pipeline = DataProcessing()

    def test_validInput(self):
        self.pipeline.add_custom_feature('BandRatioFeatureExtractor',
                                         lower_limit=None,
                                         upper_limit=50,
                                         data_key='z')
        self.assertTrue(hasattr(self.pipeline.features,
                                'band_ratio_z_50'))

    def test_nonexistentExtractorName(self):
        with self.assertRaises(ValueError):
            self.pipeline.add_custom_feature('NonExistentExtractor',
                                             lower_limit=None,
                                             upper_limit=50,
                                             data_key='z')

    def test_wrongParameterList(self):
        with self.assertRaises(ValueError):
            self.pipeline.add_custom_feature('BandRatioFeatureExtractor',
                                             lower_limit=None,
                                             upper_limit=50,
                                             non_existent_param=3)


class TestAddCustomFeatures(unittest.TestCase):

    def setUp(self):
        self.pipeline = DataProcessing()

    def test_validInput(self):
        features = [{'extractor_name': 'BandRatioFeatureExtractor',
                     'lower_limit': low,
                     'upper_limit': up} for low, up in ((None, 50), (0, 2))]
        self.pipeline.add_custom_features(features)
        self.assertTrue(hasattr(self.pipeline.features, 'band_ratio_z_50'))
        self.assertTrue(hasattr(self.pipeline.features, 'band_ratio_0_z_2'))


class TestLoad(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _input_file = 'C_43FN1_1.LAZ'

    def setUp(self):
        self._input_file_path = os.path.join('testdata',
                                             self._input_file)
        self._points_in_file = get_number_of_points_in_LAZ_file(self._input_file_path)
        self.pipeline = DataProcessing()

    def tearDown(self):
        if os.path.isdir(self._test_dir):
            shutil.rmtree(self._test_dir)

    def test_loadDataFromDirectory(self):
        os.mkdir(self._test_dir)
        shutil.copy(self._input_file_path, self._test_dir)
        self.pipeline.input_folder = self._test_dir
        self.pipeline.load()
        self.assertEqual(_get_point_cloud_size(self.pipeline.point_cloud),
                         self._points_in_file)

    def test_loadDataFromFile(self):
        self.pipeline.input_path = self._input_file_path
        self.pipeline.load()
        self.assertEqual(_get_point_cloud_size(self.pipeline.point_cloud),
                         self._points_in_file)

    def test_loadDataWithOptions(self):
        self.pipeline.input_path = self._input_file_path
        attr_to_read = ['intensity', 'gps_time']
        self.pipeline.load(attributes=attr_to_read)
        read_attr = list(self.pipeline.point_cloud['vertex'].keys())
        expected_attr = ['x', 'y', 'z'] + attr_to_read
        self.assertListEqual(read_attr, expected_attr)

    def test_provenanceIsAdded(self):
        self.pipeline.input_path = self._input_file_path
        self.pipeline.load()
        self.assertEqual(len(self.pipeline.point_cloud['log']), 1)

    def test_loadDataEmptyDirectory(self):
        os.mkdir(self._test_dir)
        self.pipeline.input_folder = self._test_dir
        with self.assertRaises(FileNotFoundError):
            self.pipeline.load()

    def test_loadDataNonexistentDirectory(self):
        self.pipeline.input_folder = self._test_dir
        with self.assertRaises(FileNotFoundError):
            self.pipeline.load()

    def test_loadDataNonexistentFile(self):
        self.pipeline.input_path = 'nonexistent_file'
        with self.assertRaises(FileNotFoundError):
            self.pipeline.load()


class TestNormalize(unittest.TestCase):

    def setUp(self):
        self.pipeline = DataProcessing()
        self.pipeline.point_cloud = create_test_point_cloud()

    def test_validCellSize(self):
        self.pipeline.normalize(10.0)
        self.assertIn('normalized_height',
                      self.pipeline.point_cloud['vertex'].keys())

    def test_provenanceIsAdded(self):
        self.pipeline.point_cloud.pop('log')
        self.pipeline.normalize(10.0)
        self.assertEqual(len(self.pipeline.point_cloud['log']), 1)

    def test_negativeCellSize(self):
        with self.assertRaises(ValueError):
            self.pipeline.normalize(-10.0)

    def test_pointCloudIsEmpty(self):
        self.pipeline.point_cloud = create_test_point_cloud(nx_values=0,
                                                            log=False)
        with self.assertRaises(RuntimeError):
            self.pipeline.normalize(10.0)


class TestApplyFiler(unittest.TestCase):

    _polygon = 'POLYGON((-0.5 -0.5, -0.5 0.5,0.5 0.5,0.5 -0.5,-0.5 -0.5))'
    _expected_points_inside = 1

    def setUp(self):
        self.pipeline = DataProcessing()
        self.pipeline.point_cloud = create_test_point_cloud(nx_values=10,
                                                            grid_spacing=1.)

    def test_selectAbove(self):
        self.pipeline.point_cloud['vertex']['feature_1']['data'][0] += 1
        self.pipeline.apply_filter('select_above',
                                   attribute='feature_1',
                                   threshold=0)
        self.assertEqual(_get_point_cloud_size(self.pipeline.point_cloud), 1)

    def test_selectBelow(self):
        self.pipeline.point_cloud['vertex']['feature_1']['data'][:-1] += 1
        self.pipeline.apply_filter('select_below',
                                   attribute='feature_1',
                                   threshold=1)
        self.assertEqual(_get_point_cloud_size(self.pipeline.point_cloud), 1)

    def test_selectEqual(self):
        self.pipeline.point_cloud['vertex']['feature_1']['data'][0] += 1
        self.pipeline.apply_filter('select_equal',
                                   attribute='feature_1',
                                   value=1)
        self.assertEqual(_get_point_cloud_size(self.pipeline.point_cloud), 1)

    def test_selectPolygon(self):
        self.pipeline.apply_filter('select_polygon',
                                   polygon_string=self._polygon)
        self.assertEqual(_get_point_cloud_size(self.pipeline.point_cloud),
                         self._expected_points_inside)

    def test_provenanceIsAdded(self):
        self.pipeline.point_cloud.pop('log')
        self.pipeline.apply_filter('select_polygon',
                                   polygon_string=self._polygon)
        self.assertEqual(len(self.pipeline.point_cloud['log']), 1)

    def test_emptyPointCloud(self):
        self.pipeline.point_cloud = create_test_point_cloud(nx_values=0,
                                                            log=False)
        with self.assertRaises(RuntimeError):
            self.pipeline.apply_filter('select_polygon',
                                       polygon_string=self._polygon)

    def test_nonexistentFilter(self):
        with self.assertRaises(ValueError):
            self.pipeline.apply_filter('nonexistent_filter')

    def test_nonexistentParameters(self):
        with self.assertRaises(TypeError):
            self.pipeline.apply_filter('select_polygon',
                                       nonexistent_parameter=1)


class TestExportPointCloud(unittest.TestCase):

    _test_dir = 'test_tmp_dir'

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = DataProcessing()
        self.pipeline.point_cloud = create_test_point_cloud(nx_values=10,
                                                            grid_spacing=1.)
        self._point_cloud_size = _get_point_cloud_size(self.pipeline.point_cloud)

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_defaultInput(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.export_point_cloud()
        self.assertListEqual(['point_cloud.ply'], os.listdir(self._test_dir))

    def test_changeOutputFilename(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.export_point_cloud(filename='tmp.ply')
        self.assertListEqual(['tmp.ply'], os.listdir(self._test_dir))

    def test_setListOfAttributes(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.export_point_cloud(attributes=['feature_1'])
        path = os.path.join(self._test_dir, 'point_cloud.ply')
        self.assertListEqual(['x', 'y', 'z', 'feature_1'],
                             _get_attributes_in_PLY_file(path))

    def test_addExportOptions(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.export_point_cloud(is_binary=True)
        with open(os.path.join(self._test_dir, 'point_cloud.ply')) as f:
            with self.assertRaises(UnicodeDecodeError):
                f.read()

    def test_outputFolderNonexistent(self):
        self.pipeline.output_folder = os.path.join(self._test_dir, 'tmp')
        with self.assertRaises(FileNotFoundError):
            self.pipeline.export_point_cloud()

    def test_emptyPointCloud(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.point_cloud = create_test_point_cloud(nx_values=0,
                                                            log=False)
        self.pipeline.export_point_cloud()
        self.assertListEqual(['point_cloud.ply'], os.listdir(self._test_dir))

    def test_filenameContainsPath(self):
        self.pipeline.output_folder = self._test_dir
        with self.assertRaises(OSError):
            self.pipeline.export_point_cloud(filename='folder/tmp.ply')


class TestExtractFeatures(unittest.TestCase):

    def setUp(self):
        self.pipeline = DataProcessing()
        self.pipeline.point_cloud = create_test_point_cloud(nx_values=10,
                                                            grid_spacing=1.,
                                                            offset=0.5)
        self.pipeline.targets = create_test_point_cloud(nx_values=5,
                                                        grid_spacing=2.,
                                                        offset=1.)
        self._feature = 'point_density'
        self._input = {'volume_type': 'cell',
                       'volume_size': 2.,
                       'feature_names': [self._feature]}

    def test_validInput(self):
        self.pipeline.extract_features(**self._input)
        self.assertIn(self._feature, self.pipeline.targets['vertex'].keys())

    def test_downsamplePointCloud(self):
        self.pipeline.extract_features(**self._input, sample_size=1)
        density = self.pipeline.targets['vertex']['point_density']['data']
        np.testing.assert_allclose(density, 0.25)

    def test_provenanceIsAdded(self):
        self.pipeline.point_cloud.pop('log')
        self.pipeline.targets.pop('log')
        self.pipeline.extract_features(**self._input)
        self.assertEqual(len(self.pipeline.targets['log']), 1)

    def test_provenanceOfPointCloudIsTransferred(self):
        self.pipeline.targets.pop('log')
        self.pipeline.extract_features(**self._input)
        self.assertEqual(len(self.pipeline.targets['log']), 2)

    def test_volumeTypeNonexistent(self):
        input = self._input.copy()
        input['volume_type'] = 'nonexistent_volume'
        with self.assertRaises(ValueError):
            self.pipeline.extract_features(**input)

    def test_FeatureNamesNonexistent(self):
        input = self._input.copy()
        input['feature_names'] = ['nonexistent_feature']
        with self.assertRaises(ValueError):
            self.pipeline.extract_features(**input)

    def test_emptyPointCloud(self):
        self.pipeline.point_cloud = create_test_point_cloud(nx_values=0,
                                                            log=False)
        self.pipeline.extract_features(**self._input)
        self.assertIn(self._feature, self.pipeline.targets['vertex'].keys())

    def test_emptyTargets(self):
        self.pipeline.targets = create_test_point_cloud(nx_values=0, log=False)
        self.pipeline.extract_features(**self._input)
        self.assertIn(self._feature, self.pipeline.targets['vertex'].keys())

    def test_emptyPointCloudAndEmptyTargets(self):
        self.pipeline.point_cloud = create_test_point_cloud(nx_values=0,
                                                            log=False)
        self.pipeline.targets = create_test_point_cloud(nx_values=0, log=False)
        self.pipeline.extract_features(**self._input)
        self.assertIn(self._feature, self.pipeline.targets['vertex'].keys())


class TestGenerateTargets(unittest.TestCase):

    def setUp(self):
        self.pipeline = DataProcessing()
        self.pipeline.point_cloud = create_test_point_cloud(nx_values=10,
                                                            grid_spacing=1.)
        self._input = {'min_x': 0.,
                       'min_y': 0.,
                       'max_x': 100.,
                       'max_y': 100.,
                       'n_tiles_side': 10,
                       'tile_mesh_size': 1.}
        self._expected_target_size = 100

    def test_validInput(self):
        self.pipeline._tile_index = (0, 0)
        self.pipeline.generate_targets(**self._input)
        self.assertEqual(_get_point_cloud_size(self.pipeline.targets),
                         self._expected_target_size)

    def test_wrongTileSelected(self):
        self.pipeline._tile_index = (1, 0)
        with self.assertRaises(AssertionError):
            self.pipeline.generate_targets(**self._input)

    def test_validationRaiseError(self):
        x_array = self.pipeline.point_cloud['vertex']['x']['data']
        mask = np.isclose(x_array, 0.)
        x_array[mask] -= 0.05
        self.pipeline._tile_index = (0, 0)
        with self.assertRaises(AssertionError):
            self.pipeline.generate_targets(**self._input)

    def test_setValidationPrecision(self):
        x_array = self.pipeline.point_cloud['vertex']['x']['data']
        mask = np.isclose(x_array, 0.)
        x_array[mask] -= 0.05
        self.pipeline._tile_index = (0, 0)
        self.pipeline.generate_targets(validate_precision=0.1, **self._input)
        self.assertEqual(_get_point_cloud_size(self.pipeline.targets),
                         self._expected_target_size)

    def test_skipValidation(self):
        input = self._input.copy()
        self.pipeline._tile_index = (0, 1)
        self.pipeline.generate_targets(validate=False, **input)
        self.assertEqual(_get_point_cloud_size(self.pipeline.targets),
                         self._expected_target_size)

    def test_emptyPointCloud(self):
        self.pipeline.point_cloud = create_test_point_cloud(nx_values=0,
                                                            log=False)
        self.pipeline._tile_index = (0, 0)
        self.pipeline.generate_targets(**self._input)
        self.assertEqual(_get_point_cloud_size(self.pipeline.targets),
                         self._expected_target_size)

    def test_tileIndexNotSet(self):
        with self.assertRaises(RuntimeError):
            self.pipeline.generate_targets(**self._input)


class TestExportTargets(unittest.TestCase):

    _test_dir = 'test_tmp_dir'

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = DataProcessing()
        self.pipeline.targets = create_test_point_cloud(nx_values=10,
                                                        grid_spacing=1.)
        self.pipeline._tile_index = (0, 0)
        self._output_name = 'tile_{}_{}.ply'.format(*self.pipeline._tile_index)
        self._point_cloud_size = _get_point_cloud_size(self.pipeline.targets)

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_defaultInput(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.export_targets()
        self.assertListEqual([self._output_name], os.listdir(self._test_dir))

    def test_changeOutputFilename(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.export_targets(filename='tmp.ply')
        self.assertListEqual(['tmp.ply'], os.listdir(self._test_dir))

    def test_setListOfAttributes(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.export_targets(attributes=['feature_1'])
        path = os.path.join(self._test_dir, self._output_name)
        self.assertListEqual(['x', 'y', 'z', 'feature_1'],
                             _get_attributes_in_PLY_file(path))

    def test_exportMultipleSingleBandFiles(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.export_targets(multi_band_files=False)
        for feature in ['feature_1', 'feature_2']:
            output_name = os.path.join(feature, self._output_name)
            output_path = os.path.join(self._test_dir, output_name)
            self.assertListEqual(['x', 'y', 'z', feature],
                                 _get_attributes_in_PLY_file(output_path))

    def test_setListOfAttributesAndMultipleSingleBandFiles(self):
        self.pipeline.output_folder = self._test_dir
        feature = 'feature_1'
        self.pipeline.export_targets(attributes=[feature],
                                     multi_band_files=False)
        output_name = os.path.join(feature, self._output_name)
        path = os.path.join(self._test_dir, output_name)
        self.assertListEqual(['x', 'y', 'z', feature],
                             _get_attributes_in_PLY_file(path))

    def test_addExportOptions(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.export_targets(is_binary=True)
        with open(os.path.join(self._test_dir, self._output_name)) as f:
            with self.assertRaises(UnicodeDecodeError):
                f.read()

    def test_outputFolderNonexistent(self):
        self.pipeline.output_folder = os.path.join(self._test_dir, 'tmp')
        with self.assertRaises(FileNotFoundError):
            self.pipeline.export_targets()

    def test_emptyTargets(self):
        self.pipeline.output_folder = self._test_dir
        self.pipeline.targets = create_test_point_cloud(nx_values=0,
                                                        log=False)
        self.pipeline.export_targets()
        self.assertListEqual([self._output_name], os.listdir(self._test_dir))

    def test_filenameContainsPath(self):
        self.pipeline.output_folder = self._test_dir
        with self.assertRaises(OSError):
            self.pipeline.export_targets(filename='folder/tmp.ply')


def _get_point_cloud_size(point_cloud):
    points = point_cloud['vertex']
    x_array = points['x']['data']
    return x_array.size


def _get_attributes_in_PLY_file(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()
    return [line.split()[2] for line in lines if line.startswith('property')]
