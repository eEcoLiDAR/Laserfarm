import os
import pathlib
import shutil
import unittest

import laspy

from laserfarm.retiler import Retiler


class TestSplitAndRedistribute(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _input_file = 'C_43FN1_1.LAZ'
    _grid_input = (-113107.8100, 214783.8700, 398892.1900, 726783.87, 256)

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = Retiler()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_correctInput(self):
        self.pipeline.input_path = pathlib.Path('testdata').joinpath(self._input_file)
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        self.pipeline.grid.setup(*self._grid_input)
        self.pipeline.split_and_redistribute()
        self.assertListEqual([d for d in sorted(os.listdir(self._test_dir))
                              if d.startswith('tile_')],
                             ["tile_101_101", "tile_101_102"])

    def test_overrideSRS(self):
        self.pipeline.input_path = pathlib.Path('testdata').joinpath(self._input_file)
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        self.pipeline.grid.setup(*self._grid_input)
        self.pipeline.split_and_redistribute(override_srs="EPSG:28992")
        for filepath in pathlib.Path(self._test_dir).glob("*/*.LAZ"):
            with laspy.open(filepath) as file:
                epsg = file.header.parse_crs().to_epsg()
                self.assertEqual(epsg, 28992)

    def test_inputFileNotSet(self):
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        self.pipeline.grid.setup(*self._grid_input)
        with self.assertRaises(OSError):
            self.pipeline.split_and_redistribute()

    def test_gridNotSet(self):
        self.pipeline.input_path = pathlib.Path('testdata').joinpath(self._input_file)
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        with self.assertRaises(ValueError):
            self.pipeline.split_and_redistribute()


class TestValidate(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _input_file = 'C_43FN1_1.LAZ'
    _grid_input = (-113107.8100, 214783.8700, 398892.1900, 726783.87, 256)

    def setUp(self):
        os.mkdir(self._test_dir)
        for n_tile, tile in enumerate(('tile_101_101', 'tile_101_102')):
            src = os.path.join('testdata',
                               self._input_file.replace('.LAZ', '_{}.LAZ'.format(n_tile+1)))
            dest = os.path.join(self._test_dir, tile)
            os.mkdir(dest)
            shutil.copy(src, dest)
        self.pipeline = Retiler()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    @property
    def json_record_file_path(self):
        json_filename = _get_retile_record_filename(self._input_file)
        return os.path.join(self._test_dir, json_filename)

    def test_correctInput(self):
        self.pipeline.input_path = pathlib.Path('testdata').joinpath(self._input_file)
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        self.pipeline.grid.setup(*self._grid_input)
        self.pipeline.validate()
        self.assertTrue(os.path.isfile(self.json_record_file_path))

    def test_correctInputNoWriteRecord(self):
        self.pipeline.input_path = pathlib.Path('testdata').joinpath(self._input_file)
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        self.pipeline.grid.setup(*self._grid_input)
        self.pipeline.validate(write_record_to_file=False)
        self.assertFalse(os.path.isfile(self.json_record_file_path))

    def test_inputFileNotSet(self):
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        self.pipeline.grid.setup(*self._grid_input)
        with self.assertRaises(OSError):
            self.pipeline.validate()

    def test_inputFileNonexistent(self):
        self.pipeline.input_path = pathlib.Path('testdata').joinpath('tmp.LAZ')
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        self.pipeline.grid.setup(*self._grid_input)
        with self.assertRaises(FileNotFoundError):
            self.pipeline.validate()

    def test_outputFolderNonexistent(self):
        self.pipeline.input_path = pathlib.Path('testdata').joinpath(self._input_file)
        self.pipeline.output_folder = 'nonexistent_dir'
        self.pipeline.grid.setup(*self._grid_input)
        with self.assertRaises(FileNotFoundError):
            self.pipeline.validate()

    def test_gridNotSet(self):
        self.pipeline.input_path = pathlib.Path('testdata').joinpath(self._input_file)
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        with self.assertRaises(ValueError):
            self.pipeline.validate()


def _get_retile_record_filename(input_filename):
    stem_input_file = os.path.splitext(input_filename)[0]
    return "_".join([stem_input_file, "retile_record.js"])








