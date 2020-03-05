import sys
import dask.bag
from dask.delayed import delayed

from pipeline import Pipeline


class MacroPipeline(object):
    _tasks = None

    @property
    def tasks(self):
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
        assert isinstance(task, Pipeline)
        self.tasks.append(task)
        return self

    @staticmethod
    def _run_task(f):
        try:
            f()
            exctype, value = (None, None)
        except:
            # sys.exc_info() provides info about current exception,
            # thus it must remain in the except block!
            exctype, value = sys.exc_info()[:2]
        return [(exctype, value)]

    def run(self):
        delayed_tasks = [delayed(self._run_task)(task.run)
                         for task in self.tasks]
        bag = dask.bag.from_delayed(delayed_tasks)
        return bag.compute()
