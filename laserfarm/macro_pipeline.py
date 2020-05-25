import logging
import sys
import traceback

from dask.distributed import Client, LocalCluster, SSHCluster

from laserfarm.pipeline import Pipeline


logger = logging.getLogger(__name__)


class MacroPipeline(object):
    """
    Class to setup macro pipeline workflows. Each MacroPipeline object entails
    multiple tasks that correspond to Pipeline instances. All the tasks are run
    in parallel using dask.

    We implement dask for embarrassingly parallel tasks, see
    https://examples.dask.org/applications/embarrassingly-parallel.html

    Example:
        >>> class Foo(Pipeline):
        ...     def __init__(self):
        ...         self.pipeline = ['task_info']
        ...     def task_info(self):
        ...         print(os.getpid())
        >>> macro = MacroPipeline()
        >>> one, two = Foo(), Foo()
        >>> macro.tasks = [one, two]
        >>> macro.run() # the default thread parallelism is used
        61244
        61245
    """
    def __init__(self):
        self._tasks = list()
        self.errors = list()
        self.client = None

    @property
    def tasks(self):
        """ List of tasks that need to be run. """
        return self._tasks

    @tasks.setter
    def tasks(self, tasks):
        try:
            _ = iter(tasks)
        except TypeError:
            logger.error('The collection of tasks should be an iterable object.')
            raise
        for task in tasks:
            assert isinstance(task, Pipeline), \
                'Task {} is not a derived Pipeline object'.format(task)
        self._tasks = [task for task in tasks]

    def add_task(self, task):
        """
        Add pipeline instance to the collection of tasks to be executed.

        :param task: Pipeline instance.
        """
        assert isinstance(task, Pipeline)
        self.tasks.append(task)
        return self

    def set_labels(self, labels):
        labels_ = [labels]*len(self.tasks) if isinstance(labels, str) else labels
        try:
            _ = iter(labels_)
        except TypeError:
            logger.error('The labels provided should be an iterable object.')
            raise
        assert len(labels_) == len(self.tasks), ('labels length does not match'
                                                 'the number of pipelines!')
        for pipeline, label in zip(self.tasks, labels_):
            pipeline.label = label

    @staticmethod
    def _run_task(f):
        exctype, value = (None, None)
        try:
            _ = f()
        except:
            # sys.exc_info() provides info about current exception,
            # thus it must remain in the except block!
            traceback.print_exc()
            exctype, value = sys.exc_info()[:2]
        return (exctype, value)

    def setup_cluster(self, mode='local', cluster=None, **kwargs):
        if self.client is not None:
            raise ValueError('Client is already set - call shutdown first!')

        if cluster is None: 
            if mode == 'local':
                cluster = LocalCluster(**kwargs)
            elif mode == 'ssh':
                cluster = SSHCluster(**kwargs)
            elif mode == 'slurm':
                raise NotImplementedError('Slurm cluster is not implemented'
                                          ' in this version!')
            else:
                raise RuntimeError('Unknown mode of setup client '
                                   '{}!'.format(mode))
        self.client = Client(cluster)

    def run(self):
        """ Run the macro pipeline. """
        futures = [self.client.submit(self._run_task, task.run)
                   for task in self.tasks]
        self.errors = self.client.gather(futures)

    def print_outcome(self, to_file=None):
        """
        Write outcome of the tasks run. If a file path is not specified, the
        log outcome is printed to the standard output.

        :param to_file: file path
        """
        fd = sys.stdout if to_file is None else open(to_file, 'w')
        for nt, (err, task) in enumerate(zip(self.errors, self.tasks)):
            if err == (None, None):
                outcome = 'Completed'
            else:
                outcome = 'Error: {}, {}'.format(err[0].__name__, err[1])
            fd.write('{:03d} {:30s} {}\n'.format(nt+1, task.label, outcome))
        if to_file is not None:
            fd.close()

    def get_failed_pipelines(self):
        return [task for err, task in zip(self.errors, self.tasks)
                if err != (None, None)]

    def shutdown(self):
        address = self.client.scheduler.address  # get adress
        self.client.close()
        Client(address).shutdown()
