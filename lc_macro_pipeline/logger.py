import logging
import pathlib
import sys


_default_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


class Logger(object):
    """ Manage the log of the (macro) pipelines. """

    def __init__(self):
        # Make a local copy of the stdout/stderr objects to configure stream handler
        self.stream_dict = {'stderr': sys.stderr, 'stdout': sys.stdout}

        self.level = 'DEBUG'
        self.formatter = logging.Formatter(_default_format)
        self.stream = self.stream_dict['stderr']
        self.filename = None

        self.logger = logging.getLogger('lc_macro_pipeline')
        self.logger.setLevel(self.level)
        self.add_stream()  # Initialize the logger with a stream

    def config(self, level=None, format=None, stream=None, filename=None):
        """
        Configure logger and update existing handlers.

        :param level:
        :param format:
        :param stream:
        :param filename:
        """
        if level is not None:
            self.level = level.upper()
            self.logger.setLevel(self.level)
        if format is not None:
            self.formatter = logging.Formatter(format)
        if stream is not None:
            self.stream = self.stream_dict[stream]
        if filename is not None:
            self.filename = filename
        self.update_handlers()

    def update_handlers(self):
        """ Update handler instances. """
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                self.logger.removeHandler(handler)
                self.add_stream()
            if isinstance(handler, logging.FileHandler):
                self.logger.removeHandler(handler)
                self.add_file(append=True)

    def add_stream(self):
        """ Add a stream handler to the log. """
        sh = logging.StreamHandler(self.stream)
        sh.setFormatter(self.formatter)
        sh.setLevel(self.level)
        self.logger.addHandler(sh)

    def add_file(self, directory='', append=False):
        """
        Add a file handler to the log. STDOUT and STDERR are also redirected to
        the log file.

        :param directory: Directory where to write the logfile.
        :param append: If True, append logs to file (if existing).
        """
        if self.filename is not None:
            _filename = self.filename
        else:
            _filename = 'lc_macro_pipeline.log'
        file_path = pathlib.Path(directory).joinpath(_filename)
        fh = logging.FileHandler(file_path,
                                 mode='w' if not append else 'a',
                                 delay=True)
        fh.setFormatter(self.formatter)
        fh.setLevel(self.level)
        self.logger.addHandler(fh)
        self.logger.debug('Start streaming to logfile: '
                          '{}'.format(file_path.as_posix()))

        # redirect stdout/stderr to log file
        sys.stdout = Log(sys.stdout, self.logger, logging.INFO)
        sys.stderr = Log(sys.stderr, self.logger, logging.ERROR)


class Log(object):
    def __init__(self, stream, logger, level):
        self.stream = stream
        self.logger = logger
        self.level = level

    def write(self, msg, *args, **kwargs):
        if msg.strip():
            self.logger.log(self.level, " ".join(msg.split()))

    def flush(self, *args, **kwargs):
        for handler in self.logger.handlers:
            handler.flush()

