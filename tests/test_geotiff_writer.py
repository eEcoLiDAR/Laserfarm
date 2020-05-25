import os
import pathlib
import shutil
import unittest

from laserfarm.geotiff_writer import GeotiffWriter

from .tools import write_PLY_targets


class test_parsePointCloud(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _tile_indices = [(0, 0), (0, 1)]
    _n_points_per_tile_and_dim = 10
    _grid_spacing = 10

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = GeotiffWriter()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_tileList(self):
        write_PLY_targets(self._test_dir,
                          indices=self._tile_indices,
                          grid_spacing=self._grid_spacing,
                          nx_values=self._n_points_per_tile_and_dim)
        self.pipeline.input_folder = self._test_dir
        self.pipeline.parse_point_cloud()
        expected_tile_list = ['tile_{}_{}.ply'.format(nx, ny)
                              for (nx, ny) in self._tile_indices]
        self.assertListEqual(sorted(self.pipeline.InputTiles),
                             sorted(expected_tile_list))

    def test_lengthDataRecord(self):
        write_PLY_targets(self._test_dir,
                          indices=self._tile_indices,
                          grid_spacing=self._grid_spacing,
                          nx_values=self._n_points_per_tile_and_dim)
        self.pipeline.input_folder = self._test_dir
        self.pipeline.parse_point_cloud()
        expected_lenght = self._n_points_per_tile_and_dim**2
        self.assertEqual(self.pipeline.LengthDataRecord,
                         expected_lenght)

    def test_resolution(self):
        write_PLY_targets(self._test_dir,
                          indices=self._tile_indices,
                          grid_spacing=self._grid_spacing,
                          nx_values=self._n_points_per_tile_and_dim)
        self.pipeline.input_folder = self._test_dir
        self.pipeline.parse_point_cloud()
        self.assertEqual(self.pipeline.xResolution, self._grid_spacing)
        self.assertEqual(self.pipeline.yResolution, self._grid_spacing)

    def test_singleBandFiles(self):
        write_PLY_targets(self._test_dir,
                          indices=self._tile_indices,
                          grid_spacing=self._grid_spacing,
                          nx_values=self._n_points_per_tile_and_dim,
                          feature='feature_1')
        self.pipeline.input_folder = self._test_dir
        self.pipeline.parse_point_cloud()
        expected_lenght = self._n_points_per_tile_and_dim**2
        self.assertEqual(self.pipeline.LengthDataRecord,
                         expected_lenght)

    def test_singlePointPLYFile(self):
        write_PLY_targets(self._test_dir,
                          indices=self._tile_indices,
                          grid_spacing=self._grid_spacing,
                          nx_values=1)
        self.pipeline.input_folder = self._test_dir
        with self.assertRaises(ValueError):
            self.pipeline.parse_point_cloud()

    def test_inputFolderNonexistent(self):
        self.pipeline.input_folder = os.path.join(self._test_dir, 'tmp')
        with self.assertRaises(FileNotFoundError):
            self.pipeline.parse_point_cloud()

    def test_emptyDirectory(self):
        self.pipeline.input_folder = self._test_dir
        with self.assertRaises(IOError):
            self.pipeline.parse_point_cloud()


class test_DataSplit(unittest.TestCase):

    _indices = [(100, 101), (101, 100), (101, 101), (103, 103)]

    def setUp(self):
        self.pipeline = GeotiffWriter()

    def test_validInput(self):
        _tiles = ['tile_{}_{}.ply'.format(nx, ny) for (nx, ny) in self._indices]
        self.pipeline.InputTiles = _tiles
        self.pipeline.data_split(len(self._indices), len(self._indices))
        for tile in _tiles:
            self.assertIn([tile], self.pipeline.subtilelists)

    def test_emptyInputTiles(self):
        self.pipeline.InputTiles = []
        with self.assertRaises(ValueError):
            self.pipeline.data_split(2, 2)

    def test_inputTilesNotSet(self):
        with self.assertRaises(ValueError):
            self.pipeline.data_split(2, 2)


class TestCreateSubregionGeotiffs(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _tile_indices = [(100, 100), (100, 101), (101, 100), (101, 101)]
    _n_points_per_tile_and_dim = 10
    _grid_spacing = 10

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = GeotiffWriter()
        self.pipeline.input_folder = pathlib.Path(self._test_dir)
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        self.pipeline.LengthDataRecord = self._n_points_per_tile_and_dim**2
        self.pipeline.xResolution = self._grid_spacing
        self.pipeline.yResolution = self._grid_spacing
        self.pipeline.subtilelists = [['tile_{}_{}.ply'.format(nx, ny)]
                                      for (nx, ny) in self._tile_indices]

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_validInput(self):
        write_PLY_targets(self._test_dir,
                          indices=self._tile_indices,
                          grid_spacing=self._grid_spacing,
                          nx_values=self._n_points_per_tile_and_dim)
        self.pipeline.bands = ['feature_1']
        self.pipeline.create_subregion_geotiffs('geotiff')
        self.assertEqual(len([f for f in os.listdir(self._test_dir)
                              if f.startswith('geotiff')]), 4)

    def test_validInputOneSubregion(self):
        write_PLY_targets(self._test_dir,
                          indices=self._tile_indices,
                          grid_spacing=self._grid_spacing,
                          nx_values=self._n_points_per_tile_and_dim)
        self.pipeline.subtilelists = [['tile_{}_{}.ply'.format(nx, ny)
                                      for (nx, ny) in self._tile_indices]]
        self.pipeline.bands = ['feature_1']
        self.pipeline.create_subregion_geotiffs('geotiff')
        self.assertEqual(len([f for f in os.listdir(self._test_dir)
                              if f.startswith('geotiff')]), 1)

    def test_emptySubTileList(self):
        self.pipeline.subtilelists = []
        self.pipeline.bands = ['feature_1']
        self.pipeline.create_subregion_geotiffs('geotiff')
        self.assertEqual(len([f for f in os.listdir(self._test_dir)
                              if f.startswith('geotiff')]), 0)

    def test_inputFolderNonexistent(self):
        self.pipeline.input_folder = pathlib.Path(self._test_dir).joinpath('tmp')
        self.pipeline.bands = ['feature_1']
        with self.assertRaises(FileNotFoundError):
            self.pipeline.create_subregion_geotiffs('geotiff')

    def test_outputFolderNonexistent(self):
        self.pipeline.output_folder = pathlib.Path(self._test_dir).joinpath('tmp')
        self.pipeline.bands = ['feature_1']
        with self.assertRaises(FileNotFoundError):
            self.pipeline.create_subregion_geotiffs('geotiff')









