import os
import shutil
import unittest

from dask.distributed import LocalCluster

from lc_macro_pipeline.macro_pipeline import MacroPipeline
from lc_macro_pipeline.pipeline import Pipeline

from .tools import ShortIOPipeline

class TestMacroPipelineObject(unittest.TestCase):

    _tmp_dask_worker_dir = 'dask-worker-space'

    def tearDown(self):
        if os.path.isdir(self._tmp_dask_worker_dir):
            shutil.rmtree(self._tmp_dask_worker_dir)

    def test_tasksDefault(self):
        mp = MacroPipeline()
        self.assertTrue(isinstance(mp.tasks, list))
        self.assertTrue(len(mp.tasks) == 0)

    def test_setTasksNotValid(self):
        mp = MacroPipeline()
        pip = Pipeline()
        with self.assertRaises(TypeError):
            mp.tasks = 0
        with self.assertRaises(TypeError):
            mp.tasks = pip
        with self.assertRaises(AssertionError):
            mp.tasks = ['load']

    def test_addTaskNotValid(self):
        mp = MacroPipeline()
        with self.assertRaises(AssertionError):
            mp.add_task(0)
        with self.assertRaises(AssertionError):
            mp.add_task('load')
        with self.assertRaises(AssertionError):
            mp.add_task(['load'])

class TestToyMacroPipeline(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _tmp_dask_worker_dir = 'dask-worker-space'

    def setUp(self):
        os.mkdir(self._test_dir)
        self.cluster = LocalCluster(processes=True,
                                    n_workers=2,
                                    threads_per_worker=1)

    def tearDown(self):
        shutil.rmtree(self._test_dir)
        if os.path.isdir(self._tmp_dask_worker_dir):
            shutil.rmtree(self._tmp_dask_worker_dir)
        self.cluster.close()

    def test_runValidPipelines(self):
        a, b = ShortIOPipeline(), ShortIOPipeline()
        file_a, file_b = [os.path.join(self._test_dir, 'file_{}.txt'.format(s))
                          for s in 'ab']
        text = 'hello world'
        a.input = {'open': file_a,
                   'write': [text],
                   'close': {}}
        b.input = {'open': file_b,
                   'write': [text],
                   'close': {}}
        mp = MacroPipeline()
        mp.tasks = [a, b]
        mp.setup_client(cluster=self.cluster)
        mp.run()
        self.assertTrue(all([os.path.isfile(f) for f in [file_a, file_b]]))
        lines_a, lines_b = [open(f).readlines() for f in [file_a, file_b]]
        self.assertEqual(lines_a, lines_b)

    def test_runInvalidPipeline(self):
        a, b = ShortIOPipeline(), ShortIOPipeline()
        file = os.path.join(self._test_dir, 'file_a.txt')
        text = 'hello world'
        a.input = {'open': file,
                   'write': [text],
                   'close': {}}
        b.input = {'open': self._test_dir,
                   'write': [text],
                   'close': {}}
        mp = MacroPipeline()
        mp.tasks = [a, b]
        mp.setup_client(cluster=self.cluster)
        errs = mp.run()
        self.assertListEqual(list(errs[0]), [None, None])
        self.assertTrue(errs[1][0], IsADirectoryError)
