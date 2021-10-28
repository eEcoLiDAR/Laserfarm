import json
import os
import shutil
import unittest

from laserfarm.utils import check_dir_exists, check_file_exists, \
    check_path_exists, get_args_from_configfile, shell_execute_cmd, DictToObj


class TestCheckLocalFS(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _test_file = 'test_temp_file.txt'
    _test_file_path = os.path.join(_test_dir, _test_file)
    _test_file_non_existent = '/test_temp_file.txt'
    _test_dir_non_existent = os.path.join(_test_dir, 'test_subdir')

    def setUp(self):
        os.mkdir(self._test_dir)
        with open(self._test_file_path, 'w') as f:
            f.write('hello world')

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_checkPathExists(self):
        check_path_exists(self._test_dir, True)
        check_path_exists(self._test_file_path, True)
        check_path_exists(self._test_file_non_existent, False)
        with self.assertRaises(FileExistsError):
            check_path_exists(self._test_file_path, False)
        with self.assertRaises(FileExistsError):
            check_path_exists(self._test_dir, False)
        with self.assertRaises(FileNotFoundError):
            check_path_exists(self._test_file_non_existent, True)

    def test_checkFileExists(self):
        check_file_exists(self._test_file_path, True)
        check_file_exists(self._test_file_non_existent, False)
        with self.assertRaises(OSError):
            check_file_exists(self._test_dir, True)
        with self.assertRaises(OSError):
            check_file_exists(self._test_dir, False)
        with self.assertRaises(FileExistsError):
            check_file_exists(self._test_file_path, False)
        with self.assertRaises(FileNotFoundError):
            check_file_exists(self._test_file_non_existent, True)

    def test_checkDirExists(self):
        check_dir_exists(self._test_dir, True)
        check_dir_exists(self._test_file_non_existent, False)
        check_dir_exists(self._test_dir_non_existent, True, mkdir=True)
        self.assertTrue(os.path.isdir(self._test_dir_non_existent))
        with self.assertRaises(FileExistsError):
            check_dir_exists(self._test_dir, False)
        with self.assertRaises(NotADirectoryError):
            check_dir_exists(self._test_file_path, True)
        with self.assertRaises(FileExistsError):
            check_dir_exists(self._test_file_path, False)
        with self.assertRaises(FileNotFoundError):
            check_dir_exists(self._test_file_non_existent, True)


class TestGetArgsFromConfigFile(unittest.TestCase):

    _test_dir = 'test_tmp_dir'

    def setUp(self):
        self.input = {'foo': {'a': 1}, 'bar': ['test', 1.4]}
        os.mkdir(self._test_dir)

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_fileDoesNotExist(self):
        _test_file_path = os.path.join(self._test_dir, 'config.config')
        with self.assertRaises(FileNotFoundError):
            get_args_from_configfile(_test_file_path)

    def test_invalidFormat(self):
        _test_file_path = os.path.join(self._test_dir, 'config.config')
        with open(_test_file_path, 'w') as fd:
            fd.write(str(self.input))
        with self.assertRaises(NotImplementedError):
            get_args_from_configfile(_test_file_path)

    def test_configFromJSON(self):
        _test_file_path = os.path.join(self._test_dir, 'config.json')
        with open(_test_file_path, 'w') as fd:
            json.dump(self.input, fd)
        self.assertDictEqual(get_args_from_configfile(_test_file_path),
                             self.input)


class TestShellExecuteCmd(unittest.TestCase):
    def test_onlyStdout(self):
        s = "HelloWorld"
        res = shell_execute_cmd(f"echo {s}")
        self.assertEqual(res[0], 0)
        self.assertEqual(res[1].split()[0], s)

    def test_onlyStderr(self):
        s = "HelloWorld"
        res = shell_execute_cmd(f"echo {s} 1>&2")
        self.assertEqual(res[0], 0)
        self.assertEqual(res[1].split()[0], s)

    def test_nonzeroReturncode(self):
        res = shell_execute_cmd("exit 1")
        self.assertTupleEqual(res, (1, '\n'))


class TestDictToObj(unittest.TestCase):
    def test_emptyDict(self):
        obj = DictToObj({})
        self.assertIsInstance(obj, DictToObj)

    def test_validDict(self):
        obj = DictToObj({'a': 5})
        self.assertTrue(hasattr(obj, 'a'))
        self.assertEqual(obj.a, 5)

    def test_invalidDict(self):
        with self.assertRaises(AttributeError):
            DictToObj(['a', 5])
