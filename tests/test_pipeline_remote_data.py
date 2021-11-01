import os
import pathlib
import shutil
import unittest

from webdav3.client import Client
from unittest.mock import patch

from laserfarm.pipeline_remote_data import PipelineRemoteData
from laserfarm.logger import Logger

from .tools import ShortPipelineRemoteData


class TestSetupLocalFS(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _test_filename = 'file.txt'
    _test_filepath = os.path.join(_test_dir, _test_filename)

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = PipelineRemoteData()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_noInputFile(self):
        self.pipeline.setup_local_fs(self._test_dir, self._test_dir)
        self.assertIsInstance(self.pipeline.input_folder, pathlib.Path)
        self.assertIsInstance(self.pipeline.output_folder, pathlib.Path)

    def test_logfileIsCreated(self):
        self.pipeline.logger = Logger()
        self.pipeline.logger.config(filename=self._test_filename,
                                    level='DEBUG')
        self.pipeline.setup_local_fs(self._test_dir, self._test_dir)
        self.pipeline.logger.terminate()
        self.assertTrue(os.path.isfile(self._test_filepath))

    def test_inputDirectoryNonexistent(self):
        # should be created
        subdirname = 'tmp'
        directory = os.path.join(self._test_dir, subdirname)
        self.pipeline.setup_local_fs(directory, self._test_dir)
        self.assertTrue(os.path.isdir(directory))

    def test_outputDirectoryNonexistent(self):
        # should be created
        subdirname = 'tmp'
        directory = os.path.join(self._test_dir, subdirname)
        self.pipeline.setup_local_fs(self._test_dir, directory)
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

    @patch('laserfarm.pipeline_remote_data.pull_from_remote')
    def test_noInputPath(self, pull_from_remote):
        client = Client({})
        input_folder = pathlib.Path(self._test_dir)
        remote_origin = '/path/to/remote'
        self.pipeline._wdclient = client
        self.pipeline.input_folder = input_folder
        self.pipeline.pullremote(remote_origin)
        pull_from_remote.assert_called_once_with(client,
                                                 input_folder.as_posix(),
                                                 remote_origin)

    @patch('laserfarm.pipeline_remote_data.pull_from_remote')
    def test_withInputPath(self, pull_from_remote):
        client = Client({})
        input_folder = pathlib.Path(self._test_dir)
        remote_origin = pathlib.Path('/path/to/remote')
        input_path = self.pipeline.input_folder.joinpath(self._test_filename)
        self.pipeline._wdclient = client
        self.pipeline.input_folder = input_folder
        self.pipeline.input_path = input_path
        self.pipeline.pullremote('/path/to/remote')
        pull_from_remote.assert_called_once_with(
            client,
            input_folder.as_posix(),
            (remote_origin/self._test_filename).as_posix()
        )

    def test_webdavClientNotSet(self):
        remote_origin = '/path/to/remote'
        self.pipeline.input_folder = pathlib.Path(self._test_dir)
        with self.assertRaises(RuntimeError):
            self.pipeline.pullremote(remote_origin)


class TestPushRemote(unittest.TestCase):

    _test_dir = 'test_tmp_dir'

    def setUp(self):
        os.mkdir(self._test_dir)
        self.pipeline = PipelineRemoteData()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    @patch('laserfarm.pipeline_remote_data.push_to_remote')
    def test_validInput(self, push_to_remote):
        client = Client({})
        output_folder = pathlib.Path(self._test_dir)
        remote_origin = '/path/to/remote'
        self.pipeline._wdclient = client
        self.pipeline.output_folder = output_folder
        self.pipeline.pushremote(remote_origin)
        push_to_remote.assert_called_once_with(client,
                                               output_folder.as_posix(),
                                               remote_origin)

    def test_webdavClientNotSet(self):
        remote_origin = '/path/to/remote'
        self.pipeline.input_folder = pathlib.Path(self._test_dir)
        with self.assertRaises(RuntimeError):
            self.pipeline.pullremote(remote_origin)


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

    @patch('laserfarm.pipeline_remote_data.super')
    def test_emptyPipeline(self, mock_super):
        pipeline = PipelineRemoteData()
        pipeline.run()
        mock_super().run.assert_called_once_with(pipeline=('setup_local_fs',
                                                           'setup_webdav_client',
                                                           'pullremote',
                                                           'pushremote',
                                                           'cleanlocalfs'))

    @patch('laserfarm.pipeline_remote_data.super')
    def test_pipelinePassedThrough(self, mock_super):
        pipeline = PipelineRemoteData()
        pipeline.run(pipeline=('test_task',))
        mock_super().run.assert_called_once_with(pipeline=('setup_local_fs',
                                                           'setup_webdav_client',
                                                           'pullremote',
                                                           'test_task',
                                                           'pushremote',
                                                           'cleanlocalfs'))

    @patch('laserfarm.pipeline_remote_data.super')
    def test_pipelinePresent(self, mock_super):
        pipeline = ShortPipelineRemoteData()
        pipeline.run()
        mock_super().run.assert_called_once_with(pipeline=('setup_local_fs',
                                                           'setup_webdav_client',
                                                           'pullremote',
                                                           'foo',
                                                           'bar',
                                                           'pushremote',
                                                           'cleanlocalfs'))

    @patch('laserfarm.pipeline_remote_data.super')
    def test_pipelinePresentAndPassedThrough(self, mock_super):
        pipeline = ShortPipelineRemoteData()
        pipeline.run(pipeline=('test_task',))
        mock_super().run.assert_called_once_with(pipeline=('setup_local_fs',
                                                           'setup_webdav_client',
                                                           'pullremote',
                                                           'test_task',
                                                           'pushremote',
                                                           'cleanlocalfs'))
