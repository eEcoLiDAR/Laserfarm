import os
import shutil
import unittest

from dask.distributed import LocalCluster

from laserfarm.macro_pipeline import MacroPipeline
from laserfarm.pipeline import Pipeline

from .tools import ShortIOPipeline


class TestMacroPipelineObject(unittest.TestCase):

    _tmp_dask_worker_dir = 'dask-worker-space'

    def tearDown(self):
        if os.path.isdir(self._tmp_dask_worker_dir):
            shutil.rmtree(self._tmp_dask_worker_dir)

    def test_tasksDefault(self):
        mp = MacroPipeline()
        self.assertIsInstance(mp.tasks, list)
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

    def test_setLabels(self):
        mp = MacroPipeline()
        mp.tasks = [Pipeline(), Pipeline()]
        labels = ['a', 'b']
        mp.set_labels(labels)
        self.assertListEqual(labels, [task.label for task in mp.tasks])


class TestSetupClientMacroPipeline(unittest.TestCase):

    def test_localClusterFromInput(self):
        mp = MacroPipeline()
        cluster = LocalCluster(processes=True,
                               n_workers=1,
                               threads_per_worker=1)
        mp.setup_cluster(cluster=cluster)
        self.assertEqual(mp.client.status, 'running')
        mp.client.cluster.close()
        status = mp.client.cluster.status
        if hasattr(status, "value"):
            status = status.value
        self.assertEqual(status, 'closed')

    def test_localClusterFromMethod(self):
        mp = MacroPipeline()
        mp.setup_cluster(mode='local', processes=True, n_workers=1,
                         threads_per_worker=1)
        self.assertEqual(mp.client.status, 'running')
        mp.client.cluster.close()
        status = mp.client.cluster.status
        if hasattr(status, "value"):
            status = status.value
        self.assertEqual(status, 'closed')

    def test_invalidCluster(self):
        mp = MacroPipeline()
        with self.assertRaises(RuntimeError):
            mp.setup_cluster(mode='newcluster')


class TestToyMacroPipeline(unittest.TestCase):

    _test_dir = 'test_tmp_dir'
    _tmp_dask_worker_dir = 'dask-worker-space'
    _outcome_file_path = os.path.join(_test_dir, 'outcome.out')

    def setUp(self):
        os.mkdir(self._test_dir)
        self.cluster = LocalCluster(processes=True,
                                    n_workers=1,
                                    threads_per_worker=1)

    def tearDown(self):
        shutil.rmtree(self._test_dir)
        self.cluster.close()
        if os.path.isdir(self._tmp_dask_worker_dir):
            shutil.rmtree(self._tmp_dask_worker_dir)

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
        mp.setup_cluster(cluster=self.cluster)
        mp.run()
        self.assertTrue(all([os.path.isfile(f) for f in [file_a, file_b]]))
        lines_a, lines_b = [open(f).readlines() for f in [file_a, file_b]]
        self.assertEqual(lines_a, lines_b)
        self.assertListEqual(mp.get_failed_pipelines(), [])
        mp.print_outcome(to_file=self._outcome_file_path)
        self.assertTrue(os.path.isfile(self._outcome_file_path))
        with open(self._outcome_file_path, 'r') as f:
            res = [line.split()[-1] for line in f.readlines()]
        self.assertListEqual(res, ['finished']*2)

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
        mp.setup_cluster(cluster=self.cluster)
        mp.run()
        self.assertListEqual(mp.get_failed_pipelines(), [b])
        self.assertIs(mp.errors[0], None)
        self.assertTrue(mp.errors[1][0], IsADirectoryError)
        mp.print_outcome(to_file=self._outcome_file_path)
        self.assertTrue(os.path.isfile(self._outcome_file_path))
        with open(self._outcome_file_path, 'r') as f:
            res = [line.split()[-1] for line in f.readlines()]
        self.assertEqual(res[0], 'finished')
        self.assertNotEqual(res[1], 'finished')
