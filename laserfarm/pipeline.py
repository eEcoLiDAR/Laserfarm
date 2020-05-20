import logging

from laserfarm.logger import Logger
from laserfarm.utils import get_args_from_configfile


logger = logging.getLogger(__name__)


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
    _pipeline = tuple()
    _input = dict()
    logger = None
    label = 'pipeline'

    @property
    def pipeline(self):
        """
        List containing the consecutive tasks that constitute the pipeline.
        """
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline):
        if isinstance(pipeline, str):
            pipeline = tuple([pipeline])
        try:
            _ = iter(pipeline)
        except TypeError:
            logger.error('The sequence of tasks in the pipeline '
                         'should be provided as an iterable object.')
            raise
        for task in pipeline:
            assert task in dir(self.__class__), \
                ('Error defining the pipeline: {} method not found'
                 'in class {}'.format(task, self.__class__.__name__))
        self._pipeline = tuple([task for task in pipeline])

    @property
    def input(self):
        """
        Dictionary containing the pipeline input. Each attribute entails the
        input for a pipeline method that needs to be executed.
        """
        return self._input

    @input.setter
    def input(self, input):
        if not isinstance(input, dict):
            raise TypeError("A dictionary is expected!")
        self._input = input

    def config(self, from_dict=None, from_file=None):
        """
        Set the pipeline input with a dictionary or by reading a configfile.

        :param from_dict: Input is given as a dictionary
        :param from_file: Path to the configfile
        """
        is_valid = (from_dict is None) != (from_file is None)
        assert is_valid, 'Either a dictionary or a file path should be given!'
        if from_dict is not None:
            self.input = from_dict
        elif from_file is not None:
            self.input = get_args_from_configfile(from_file)
        return self

    def log_config(self, level=None, format=None, stream=None, filename=None):
        self.logger.config(level, format, stream, filename)

    def run(self, pipeline=None):
        """
        Run the full pipeline.

        :param pipeline: (optional) Run the input pipeline if provided
        """
        _input = self.input.copy()
        _pipeline = pipeline if pipeline is not None else self.pipeline
        _pipeline = ('log_config',) + _pipeline

        self.logger = Logger(label=self.label)

        for task_name in _pipeline:
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
            logger.warning('Some of the attributes in input have not been '
                           'used: {} '.format(', '.join(_input.keys())))

        self.logger.terminate()
        return
