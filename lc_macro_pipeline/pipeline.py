from lc_macro_pipeline.utils import get_args_from_configfile


class Pipeline(object):
    """
    Base Pipeline class to construct workflows. Inheriting classes should
    define `pipeline` as the sequence of the methods that constitute the
    pipeline. After storing the input of the various tasks in the `input`
    dictionary, the pipeline can be run with the method `run`.

    Example:
         >>> class FooBar(Pipeline):
         ...    def __init__(self):
         ...        self.pipeline = ['foo', 'bar']
         ...    def foo(self, a):
         ...        print(a)
         ...    def bar(self, b):
         ...        print(b)
         >>> test = FooBar()
         >>> test.input = {'foo': {'a': 5}, 'bar': {'b': 6}}
         >>> test.run()
         5
         6
    """
    _pipeline = None
    _input = None

    @property
    def pipeline(self):
        """
        List containing the consecutive tasks that constitute the pipeline.
        """
        if self._pipeline is None:
            self._pipeline = []
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline):
        try:
            _ = iter(pipeline)
        except TypeError:
            print('The sequence of tasks in the pipeline '
                  'should be provided as an iterable object.')
            raise
        for task in pipeline:
            assert task in dir(self.__class__), \
                ('Error defining the pipeline: {} method not found'
                 'in class {}'.format(task, self.__class__.__name__))
        self._pipeline = [task for task in pipeline]

    @property
    def input(self):
        """
        Dictionary containing the pipeline input. Each attribute entails the
        input for a pipeline method that needs to be executed.
        """
        if self._input is None:
            self._input = {}
        return self._input

    @input.setter
    def input(self, input):
        if not isinstance(input, dict):
            raise TypeError("A dictionary is expected!")
        else:
            # TODO: check whether all attributes are also in pipeline?
            self._input = input

    def config(self, path):
        """
        Set the pipeline input by reading a configfile.

        :param path: Path to the configfile
        """
        self.input = get_args_from_configfile(path)
        return self

    def run(self):
        """ Run the full pipeline. """
        _input = self.input.copy()

        for task_name in self.pipeline:
            if task_name in _input:
                task = getattr(self, task_name)
                input_task = _input.pop(task_name)
                if isinstance(input_task, dict):
                    task(**input_task)
                elif (isinstance(input_task, list)
                      or isinstance(input_task, tuple)):
                    task(*input_task)
                else:
                    task(input_task)

        if len(_input.keys()) > 0:
            raise Warning('Some of the attributes in input have not been used:'
                          ' {} '.format(', '.join(_input.keys())))
        return
