import json
import os
import pathlib
import shutil
import unittest

from unittest.mock import create_autospec
from webdav3.client import Client, RemoteResourceNotFound

from laserfarm.remote_utils import get_wdclient, list_remote, \
    get_info_remote, pull_from_remote, push_to_remote, purge_local


class TestGetWdclient(unittest.TestCase):

    _test_dir = 'test_tmp_dir'

    def setUp(self):
        os.mkdir(self._test_dir)

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    @property
    def options(self):
        return {'webdav_hostname': 'http://localhost:8585',
                'webdav_login': 'alice',
                'webdav_password': 'secret1234'}

    def test_validOptionsFromDictionary(self):
        client = get_wdclient(self.options)
        self.assertIsInstance(client, Client)

    def test_validOptionsFromFile(self):
        filepath = os.path.join(self._test_dir, "options.json")
        with open(filepath, 'w') as f:
            json.dump(self.options, f)
        client = get_wdclient(filepath)
        self.assertIsInstance(client, Client)

    def test_validOptionsFromFileWithAuthenticaltionFile(self):
        options_filepath = os.path.join(self._test_dir, "options.json")
        authentication_filepath = os.path.join(self._test_dir,
                                               "credentials.json")
        options = self.options
        login = options.pop('webdav_login')
        passwd = options.pop('webdav_password')
        options.update({'authenticationfile': authentication_filepath})
        with open(options_filepath, 'w') as f:
            json.dump(options, f)
        authentication = {'webdav_login': login, 'webdav_password': passwd}
        with open(authentication_filepath, 'w') as f:
            json.dump(authentication, f)
        client = get_wdclient(options_filepath)
        self.assertIsInstance(client, Client)

    def test_webdavLoginIsNotProvided(self):
        options = self.options
        _ = options.pop('webdav_login')
        with self.assertRaises(RuntimeError):
            _ = get_wdclient(options)

    def test_bothWebdavTokenAndLoginAreProvided(self):
        options = self.options
        options['webdav_token'] = 'token123'
        with self.assertRaises(RuntimeError):
            _ = get_wdclient(options)

    def test_optionFileNonexistent(self):
        filepath = os.path.join(self._test_dir, "options.json")
        with self.assertRaises(FileNotFoundError):
            _ = get_wdclient(filepath)

    def test_invalidFileFormat(self):
        filepath = os.path.join(self._test_dir, "options.txt")
        with open(filepath, 'w') as f:
            json.dump(self.options, f)
        with self.assertRaises(NotImplementedError):
            _ = get_wdclient(filepath)

    def test_invalidOptionType(self):
        options = list(self.options.values())
        with self.assertRaises(TypeError):
            _ = get_wdclient(options)

    def test_authenticationFileNonexistent(self):
        options_filepath = os.path.join(self._test_dir, "options.json")
        authentication_filepath = os.path.join(self._test_dir,
                                               "credentials.json")
        options = self.options
        _ = options.pop('webdav_login')
        _ = options.pop('webdav_password')
        options.update({'authenticationfile': authentication_filepath})
        with open(options_filepath, 'w') as f:
            json.dump(options, f)
        with self.assertRaises(FileNotFoundError):
            _ = get_wdclient(options_filepath)


class TestListRemote(unittest.TestCase):

    def setUp(self):
        self.client = _get_mock_webdav_client()

    def test_correctMethodIsCalled(self):
        list_remote(self.client, os.getcwd())
        self.client.list.assert_called_once_with(os.getcwd())


class TestGetInfoRemote(unittest.TestCase):

    def setUp(self):
        self.client = _get_mock_webdav_client()

    def test_correctMethodIsCalled(self):
        get_info_remote(self.client, os.getcwd())
        self.client.info.assert_called_once_with(os.getcwd())


class TestPullFromRemote(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _test_local_dir = os.path.join(_test_dir, 'local')
    _test_remote_dir = os.path.join(_test_dir, 'remote')
    _test_filename = 'filename.txt'

    def setUp(self):
        os.mkdir(self._test_dir)
        os.mkdir(self._test_local_dir)
        os.mkdir(self._test_remote_dir)
        self.client = _get_mock_webdav_client()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_pullFileFromRemote(self):
        remote_path = os.path.join(self._test_remote_dir, self._test_filename)
        local_path = os.path.join(self._test_local_dir, self._test_filename)
        with open(remote_path, 'w') as f:
            f.write('hello world')
        pull_from_remote(self.client,
                         self._test_local_dir,
                         remote_path)
        self.assertTrue(os.path.isfile(local_path))

    def test_pullDirectoryFromRemote(self):
        local_dir = os.path.join(self._test_local_dir, 'remote')
        file_path = os.path.join(self._test_remote_dir, self._test_filename)
        file_path_local = os.path.join(local_dir, self._test_filename)
        with open(file_path, 'w') as f:
            f.write('hello world')
        pull_from_remote(self.client,
                         local_dir,
                         self._test_remote_dir)
        self.client.download_file.assert_called_once_with(file_path,
                                                          file_path_local)

    def test_invalidTypeForPath(self):
        remote_path = pathlib.Path(self._test_remote_dir).joinpath(self._test_filename)
        with open(remote_path, 'w') as f:
            f.write('hello world')
        with self.assertRaises(TypeError):
            pull_from_remote(self.client,
                             self._test_local_dir,
                             remote_path)

    def test_remotePathDoesNotExist(self):
        remote_path = os.path.join(self._test_remote_dir, self._test_filename)
        with self.assertRaises(RemoteResourceNotFound):
            pull_from_remote(self.client,
                             self._test_local_dir,
                             remote_path)


class TestPushToRemote(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _test_local_dir = os.path.join(_test_dir, 'local')
    _test_remote_dir = os.path.join(_test_dir, 'remote')
    _test_filename = 'filename.txt'

    def setUp(self):
        os.mkdir(self._test_dir)
        os.mkdir(self._test_local_dir)
        os.mkdir(self._test_remote_dir)
        self.client = _get_mock_webdav_client()

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_pushFileToRemote(self):
        remote_path = os.path.join(self._test_remote_dir, self._test_filename)
        local_path = os.path.join(self._test_local_dir, self._test_filename)
        with open(local_path, 'w') as f:
            f.write('hello world')
        push_to_remote(self.client,
                       local_path,
                       self._test_remote_dir)
        self.assertTrue(os.path.isfile(remote_path))

    def test_pushDirectoryToRemote(self):
        remote_dir = os.path.join(self._test_remote_dir, 'local')
        file_path = os.path.join(self._test_local_dir, self._test_filename)
        file_path_remote = os.path.join(remote_dir, self._test_filename)
        with open(file_path, 'w') as f:
            f.write('hello world')
        push_to_remote(self.client,
                       self._test_local_dir,
                       remote_dir)
        self.client.upload_sync.assert_called_once_with(file_path_remote,
                                                        file_path)

    def test_invalidTypeForPath(self):
        local_path = pathlib.Path(self._test_local_dir).joinpath(self._test_filename)
        with open(local_path, 'w') as f:
            f.write('hello world')
        with self.assertRaises(TypeError):
            push_to_remote(self.client,
                           local_path,
                           self._test_remote_dir)

    def test_localPathDoesNotExist(self):
        local_path = os.path.join(self._test_local_dir, self._test_filename)
        with self.assertRaises(FileNotFoundError):
            push_to_remote(self.client,
                           local_path,
                           self._test_remote_dir)

    def test_remoteFileExists(self):
        remote_path = os.path.join(self._test_remote_dir, 'local')
        with open(remote_path, 'w') as f:
            f.write('hello world')
        with self.assertRaises(FileExistsError):
            push_to_remote(self.client,
                           self._test_local_dir,
                           remote_path)


class TestPurgeLocal(unittest.TestCase):

    _test_dir = 'test_tmp_dir'

    def setUp(self):
        os.mkdir(self._test_dir)

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_removeDirectory(self):
        dirname = 'tmp'
        tmp_dir = os.path.join(self._test_dir, dirname)
        os.mkdir(tmp_dir)
        purge_local(tmp_dir)
        self.assertFalse(dirname in os.listdir(self._test_dir))

    def test_removeFile(self):
        filename = 'tmp.txt'
        tmp_file = os.path.join(self._test_dir, filename)
        with open(tmp_file, 'w') as f:
            f.write('hello world')
        purge_local(tmp_file)
        self.assertFalse(filename in os.listdir(self._test_dir))

    def test_nonExistentPath(self):
        filename = 'tmp.txt'
        tmp_file = os.path.join(self._test_dir, filename)
        with self.assertRaises(FileNotFoundError):
            purge_local(tmp_file)


def _get_mock_webdav_client():
    MockClient = create_autospec(Client)
    client = MockClient({})
    client.check.side_effect = os.path.exists
    client.is_dir.side_effect = os.path.isdir
    client.download_file.side_effect = shutil.copy
    client.list.side_effect = os.listdir
    client.upload_sync.side_effect = lambda x, y: shutil.copy(y, x)
    client.mkdir.side_effect = os.mkdir
    return client

