from utils import get_args_from_configfile

class Pipeline(object):

    _pipeline = None
    _input = None

    @property
    def pipeline(self):
        """
        List containing the consecutive steps that constitute the pipeline.
        """
        if self._pipeline is None:
            self._pipeline = []
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline):
        if isinstance(pipeline, list):
            self._pipeline = pipeline
        else:
            raise TypeError('A list is expected!')

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
            self._input = input

    def config(self, path):
        """
        Set the pipeline using input from a configfile.

        :param path: Path to the configfile
        """
        self.input = get_args_from_configfile(path)
        return self

    def run(self):
        """ Run the full pipeline. """
        for task_name in self.pipeline:
            if task_name in self.input:
                task = getattr(self, task_name)
                task(**self.input[task_name])
        return

