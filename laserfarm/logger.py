import logging
import pathlib
import sys

from laserfarm.utils import check_dir_exists

logger = logging.getLogger(__name__)
_default_format = '%(asctime)s - %(name)40s - %(levelname)10s - %(message)s'
# Make a local copy of the stdout/stderr objects to configure stream handler
_stream_dict = {'stderr': sys.stderr, 'stdout': sys.stdout}


class Logger(object):
    """ Manage the log of the (macro) pipelines. """

    def __init__(self, label='laserfarm'):
        self.level = 'INFO'
        self.formatter = logging.Formatter(_default_format)
        self.stream = _stream_dict['stderr']
        self.filename = pathlib.Path(label).with_suffix('.log')

        self.logger = logging.getLogger()
        self.logger.setLevel(self.level)
        self.start_log_to_stream()  # Initialize the logger with a stream

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
            self.stream = _stream_dict[stream]
        if filename is not None:
            self.filename = pathlib.Path(filename)
        self.update_handlers()

    def terminate(self):
        self.remove_handlers(stream=True, file=True)

    def update_handlers(self):
        """ Update handler instances. """
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                self.start_log_to_stream()
            if isinstance(handler, logging.FileHandler):
                self.start_log_to_file(append=True)

    def remove_handlers(self, stream=False, file=False):
        """ Remove handler instances. """
        mask = [True for h in self.logger.handlers]
        for n, handler in enumerate(self.logger.handlers):
            if isinstance(handler, logging.StreamHandler) and stream:
                mask[n] = False
            if isinstance(handler, logging.FileHandler) and file:
                mask[n] = False
                logger.debug('Terminating stream to logfile: '
                             '{}'.format(handler.baseFilename))
                self._redirect_std_streams(False)
        self.logger.handlers = [h for n, h in enumerate(self.logger.handlers)
                                if mask[n]]

    def _redirect_std_streams(self, redirect):
        if redirect:
            # redirect stdout/stderr to log file
            sys.stdout = Log(sys.stdout, self.logger, logging.INFO)
            sys.stderr = Log(sys.stderr, self.logger, logging.ERROR)
        else:
            # restore default streams
            sys.stdout = _stream_dict['stdout']
            sys.stderr = _stream_dict['stderr']

    def start_log_to_stream(self):
        """
        Add a stream handler to the log. If a stream handler was already
        present, remove it.
        """
        self.remove_handlers(stream=True)
        sh = logging.StreamHandler(self.stream)
        sh.setFormatter(self.formatter)
        sh.setLevel(self.level)
        self.logger.addHandler(sh)

    def start_log_to_file(self, directory='', append=False):
        """
        Add a file handler to the log. STDOUT and STDERR are also redirected to
        the log file. If a stream handler was already present, remove it.

        :param directory: Directory where to write the logfile (ignore it if
        filename already includes path)
        :param append: If True, append logs to file (if existing).
        """
        self.remove_handlers(file=True)

        if not self.filename.parent.name:
            file_path = pathlib.Path(directory).joinpath(self.filename.name)
        else:
            file_path = self.filename
        check_dir_exists(file_path.parent, should_exist=True)
        fh = logging.FileHandler(file_path,
                                 mode='w' if not append else 'a',
                                 delay=True)
        fh.setFormatter(self.formatter)
        fh.setLevel(self.level)
        self.logger.addHandler(fh)

        logger.debug('Start stream to file: {}'.format(file_path.as_posix()))
        self._redirect_std_streams(True)


class Log(object):
    def __init__(self, stream, logger_obj, level):
        self.stream = stream
        self.logger = logger_obj
        self.level = level

    def write(self, msg, *args, **kwargs):
        if msg.strip():
            self.logger.log(self.level, " ".join(msg.split()))

    def flush(self, *args, **kwargs):
        for handler in self.logger.handlers:
            handler.flush()

