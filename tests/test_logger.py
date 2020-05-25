import logging
import os
import shutil
import sys
import unittest

from laserfarm.logger import Logger, Log


logger = logging.getLogger(__name__)
_stream_dict = {'stderr': sys.stderr, 'stdout': sys.stdout}


class TestLoggerOnlyStream(unittest.TestCase):

    def setUp(self):
        self.logger = Logger()

    def tearDown(self):
        self.logger.terminate()

    def test_onlyOneHandler(self):
        self.assertEqual(len(self.logger.logger.handlers), 1)

    def test_handlerIsStream(self):
        self.assertIsInstance(self.logger.logger.handlers[0],
                              logging.StreamHandler)

    def test_stdoutAndStderrAreNotRedirected(self):
        self.assertEqual(sys.stdout, _stream_dict['stdout'])
        self.assertEqual(sys.stderr, _stream_dict['stderr'])


class TestLoggerSetFile(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _message = 'Hello_world'

    def setUp(self):
        os.mkdir(self._test_dir)
        self.logger = Logger()

    def tearDown(self):
        self.logger.terminate()
        shutil.rmtree(self._test_dir)

    def test_stdoutAndStderrAreRedirected(self):
        self.logger.start_log_to_file(directory=self._test_dir)
        self.assertIsInstance(sys.stdout, Log)
        self.assertIsInstance(sys.stderr, Log)

    def test_logfileContainsLogMessage(self):
        self.logger.start_log_to_file(directory=self._test_dir)
        logger.info(self._message)
        logfile = os.path.join(self._test_dir, self.logger.filename)
        self.assertTrue(os.path.isfile(logfile))
        with open(logfile, 'r') as f:
            for line in f:
                if line.split()[5] == 'INFO':
                    self.assertEqual(line.split()[3], __name__)
                    self.assertEqual(line.split()[7], self._message)

    def test_addFileNonExistentDir(self):
        non_existent_dir = os.path.join(self._test_dir, 'tmp')
        with self.assertRaises(FileNotFoundError):
            self.logger.start_log_to_file(directory=non_existent_dir)

    def test_addFileTwice(self):
        newdir = os.path.join(self._test_dir, 'tmp')
        os.mkdir(newdir)
        self.logger.start_log_to_file(directory=self._test_dir)
        logger.info(self._message)
        self.logger.start_log_to_file(directory=newdir)
        logger.info(self._message)
        for dir in [self._test_dir, newdir]:
            logfile = os.path.join(dir, self.logger.filename)
            self.assertTrue(os.path.isfile(logfile))

    def test_configSetFilename(self):
        newlogfilename = 'newlogfilename.log'
        self.logger.config(filename=newlogfilename)
        self.logger.start_log_to_file(directory=self._test_dir)
        logger.info(self._message)
        logfile = os.path.join(self._test_dir, newlogfilename)
        self.assertTrue(os.path.isfile(logfile))

    def test_configSetFormat(self):
        format = "%(name)s %(message)s %(levelname)s"
        self.logger.config(format=format, level='INFO')
        self.logger.start_log_to_file(directory=self._test_dir)
        logger.info(self._message)

        logfile = os.path.join(self._test_dir, self.logger.filename)
        with open(logfile, 'r') as f:
            line = f.read()
        self.assertEqual(line.split()[0], __name__)
        self.assertEqual(line.split()[1], self._message)
        self.assertEqual(line.split()[2], 'INFO')

    def test_configSetLevel(self):
        self.logger.config(level='info')
        self.logger.start_log_to_file(directory=self._test_dir)
        logger.debug(self._message)
        logfile = os.path.join(self._test_dir, self.logger.filename)
        self.assertFalse(os.path.isfile(logfile))

    def test_terminate(self):
        self.logger.terminate()
        self.assertListEqual(logger.handlers, [])
        self.assertEqual(sys.stdout, _stream_dict['stdout'])
        self.assertEqual(sys.stderr, _stream_dict['stderr'])



