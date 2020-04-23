import os
import pathlib
import shutil
import unittest

from unittest.mock import patch

from lc_macro_pipeline.pipeline_remote_data import PipelineRemoteData
from lc_macro_pipeline.logger import Logger

from .tools import ShortPipelineRemoteData


class TestLocalfs(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _test_filename = 'file.txt'
    _test_filepath = os.path.join(_test_dir, _test_filename)

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = PipelineRemoteData()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_noInputFile(self):
        self.pipeline.localfs(self._test_dir, self._test_dir)
        self.assertIsInstance(self.pipeline.input_folder, pathlib.Path)
        self.assertIsInstance(self.pipeline.output_folder, pathlib.Path)

    def test_withInputFile(self):
        self.pipeline.localfs(self._test_dir,
                              self._test_dir,
                              self._test_filename)
        self.assertEqual(self.pipeline.input_path.as_posix(),
                         self._test_filepath)

    def test_logfileIsCreated(self):
        self.pipeline.logger = Logger()
        self.pipeline.logger.config(filename=self._test_filename)
        self.pipeline.localfs(self._test_dir, self._test_dir)
        self.assertTrue(os.path.isfile(self._test_filepath))

    def test_inputDirectoryNonexistent(self):
        # should not be created
        subdirname = 'tmp'
        directory = os.path.join(self._test_dir, subdirname)
        self.pipeline.localfs(directory, self._test_dir)
        self.assertFalse(os.path.isdir(directory))

    def test_outputDirectoryNonexistent(self):
        # should be created
        subdirname = 'tmp'
        directory = os.path.join(self._test_dir, subdirname)
        self.pipeline.localfs(self._test_dir, directory)
        self.assertTrue(os.path.isdir(directory))


class TestPullRemote(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _test_filename = 'file.txt'
    _test_filepath = os.path.join(_test_dir, _test_filename)

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = PipelineRemoteData()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    @patch('lc_macro_pipeline.pipeline_remote_data.get_wdclient')
    @patch('lc_macro_pipeline.pipeline_remote_data.pull_from_remote')
    def test_noInputFile(self, _, pull_from_remote):
        self.pipeline.input_folder = pathlib.Path(self._test_dir)
        self.pipeline.pullremote({}, '/path/to/remote')
        pull_from_remote.assert_called_once()

    @patch('lc_macro_pipeline.pipeline_remote_data.get_wdclient')
    @patch('lc_macro_pipeline.pipeline_remote_data.pull_from_remote')
    def test_withInputFile(self, _, pull_from_remote):
        self.pipeline.input_folder = pathlib.Path(self._test_dir)
        self.pipeline.input_path = self.pipeline.input_folder.joinpath(self._test_filename)
        self.pipeline.pullremote({}, '/path/to/remote')
        pull_from_remote.assert_called_once()


class TestPushRemote(unittest.TestCase):

    _test_dir = 'test_tmp_dir'

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = PipelineRemoteData()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    @patch('lc_macro_pipeline.pipeline_remote_data.get_wdclient')
    @patch('lc_macro_pipeline.pipeline_remote_data.push_to_remote')
    def test_validInput(self, _, push_to_remote):
        self.pipeline.output_folder = pathlib.Path(self._test_dir)
        self.pipeline.pushremote({}, '/path/to/remote')
        push_to_remote.assert_called_once()


class TestPurgeLocal(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _test_input_dir = os.path.join(_test_dir, 'input')
    _test_output_dir = os.path.join(_test_dir, 'output')

    def setUp(self):
        os.mkdir(self._test_dir)
        os.mkdir(self._test_input_dir)
        os.mkdir(self._test_output_dir)
        self.pipeline = PipelineRemoteData()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_foldersExist(self):
        self.pipeline.input_folder = pathlib.Path(self._test_input_dir)
        self.pipeline.output_folder = pathlib.Path(self._test_output_dir)
        self.pipeline.cleanlocalfs()
        self.assertEqual(len(os.listdir(self._test_dir)), 0)


class TestRun(unittest.TestCase):

    @patch('lc_macro_pipeline.pipeline_remote_data.super')
    def test_emptyPipeline(self, mock_super):
        pipeline = PipelineRemoteData()
        pipeline.run()
        mock_super().run.assert_called_once_with(pipeline=('localfs',
                                                           'pullremote',
                                                           'pushremote',
                                                           'cleanlocalfs'))

    @patch('lc_macro_pipeline.pipeline_remote_data.super')
    def test_pipelinePassedThrough(self, mock_super):
        pipeline = PipelineRemoteData()
        pipeline.run(pipeline=('test_task',))
        mock_super().run.assert_called_once_with(pipeline=('localfs',
                                                           'pullremote',
                                                           'test_task',
                                                           'pushremote',
                                                           'cleanlocalfs'))

    @patch('lc_macro_pipeline.pipeline_remote_data.super')
    def test_pipelinePresent(self, mock_super):
        pipeline = ShortPipelineRemoteData()
        pipeline.run()
        mock_super().run.assert_called_once_with(pipeline=('localfs',
                                                           'pullremote',
                                                           'foo',
                                                           'bar',
                                                           'pushremote',
                                                           'cleanlocalfs'))

    @patch('lc_macro_pipeline.pipeline_remote_data.super')
    def test_pipelinePresentAndPassedThrough(self, mock_super):
        pipeline = ShortPipelineRemoteData()
        pipeline.run(pipeline=('test_task',))
        mock_super().run.assert_called_once_with(pipeline=('localfs',
                                                           'pullremote',
                                                           'test_task',
                                                           'pushremote',
                                                           'cleanlocalfs'))
