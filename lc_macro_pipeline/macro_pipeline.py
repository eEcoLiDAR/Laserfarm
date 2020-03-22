import sys

# import dask
# import dask.bag
# from dask.delayed import delayed
from dask.distributed import Client
from dask.distributed import LocalCluster

from lc_macro_pipeline.pipeline import Pipeline


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
    _tasks = list()

    @property
    def tasks(self):
        """ List of tasks that need to be run. """
        return self._tasks

    @tasks.setter
    def tasks(self, tasks):
        try:
            _ = iter(tasks)
        except TypeError:
            print('The collection of tasks should be an iterable object. ')
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

    @staticmethod
    def _run_task(f):
        exctype, value = (None, None)
        try:
            _ = f()
        except:
            # sys.exc_info() provides info about current exception,
            # thus it must remain in the except block!
            exctype, value = sys.exc_info()[:2]
        return (exctype, value)

    # def run(self):
    #     """ Run the macro pipeline. """
    #     delayed_tasks = [delayed(self._run_task)(task.run)
    #                      for task in self.tasks]
    #     # return dask.compute(*delayed_tasks)
    #     bag = dask.bag.from_delayed(delayed_tasks)
    #     return bag.compute()

    def setup_client(self, 
                     mode = 'auto',
                     address='0.0.0.0', 
                     num_workers=1, 
                     port=0, 
                     num_threads_per_worker=1, 
                     memory_limit=0,
                     silence_logs=True):
        if mode == 'auto':
            self.client = Client()
        elif mode == 'mannual':
            localcluster = LocalCluster(
                ip=address,
                scheduler_port=port,
                n_workers=num_workers,
                memory_limit=memory_limit,
                threads_per_worker=num_threads_per_worker,
                silence_logs=silence_logs
            )
            self.client = Client(localcluster)

    def run(self):
        """ Run the macro pipeline. """
        futures = [self.client.submit(self._run_task, task.run)
                   for task in self.tasks]
        results = self.client.gather(futures)
        return results
    
    def shutdown_client(self):
        self.client.shutdown()
