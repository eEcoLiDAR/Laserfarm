import sys

# import dask
# import dask.bag
# from dask.delayed import delayed
from dask.distributed import Client

from pipeline import Pipeline


class MacroPipeline(object):
    """
    Class to setup macro pipeline workflows. Each MacroPipeline object entails
    multiple tasks that correspond to Pipeline instances. All the tasks can be
    run in parallel using dask.

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
    _tasks = None

    @property
    def tasks(self):
        """ List of Pipeline instances that needs to be run. """
        if self._tasks is None:
            self._tasks = []
        return self._tasks

    @tasks.setter
    def tasks(self, tasks):
        if isinstance(tasks, list):
            if all([isinstance(task, Pipeline) for task in tasks]):
                self._tasks = tasks
            else:
                raise TypeError('All elements in the list should be Pipeline!')
        else:
            raise TypeError('List is expected!')

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
        return exctype, value

    # def run(self):
    #     """ Run the macro pipeline. """
    #     delayed_tasks = [delayed(self._run_task)(task.run)
    #                      for task in self.tasks]
    #     # return dask.compute(*delayed_tasks)
    #     bag = dask.bag.from_delayed(delayed_tasks)
    #     return bag.compute()

    def run(self):
        """ Run the macro pipeline. """
        self.client = Client()  # TODO: find best way to setup client (set method?)
        futures = [self.client.submit(self._run_task, task.run)
                   for task in self.tasks]
        return self.client.gather(futures)
