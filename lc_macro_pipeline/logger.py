import logging
import sys


class Logger(object):
    """ Manage the log of the (macro) pipelines. """

    def __init__(self, level='debug', format=None):
        self.logger = logging.getLogger('lc_macro_pipeline')
        self.level = level
        _format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s' if format is None else format
        self.formatter = logging.Formatter(_format)
        self.redirect_streams = False

    def add_stream(self, stream='stderr'):
        """
        Add a stream handler to the log.

        :param stream: Stream where to log, 'stdout' or 'stderr'.
        """
        assert stream in ['stdout', 'stderr'], ('Stream should be '
                                                '"stdout" or "stderr"')
        if self.redirect_streams:
            logging.error('Streams are already directed to file!')
            return

        sys_stream = getattr(sys, stream)
        sh = logging.StreamHandler(sys_stream)
        sh.setFormatter(self.formatter)
        sh.setLevel(self.level)
        self.logger.addHandler(sh)

    def add_file(self, filename="log.log", redirect_streams=False):
        """
        Add a file handler to the log. If we redirect streams to a file, the
        stream handler will be removed.

        :param filename: Name of the logfile.
        :param redirect_streams: if True, redirect streams to logfile. This is
        useful to capture all stdout/stderr produced by external packages.
        """
        fh = logging.FileHandler(filename, mode='w')
        fh.setFormatter(self.formatter)
        fh.setLevel(self.level)
        self.logger.addHandler(fh)

        if redirect_streams:
            logging.warning('Removing streamHandler from logger. STDOUT/STDERR'
                            'will be now directed only to {}'.format(filename))
            self.remove_handler('stream')

            sys.stdout.write = lambda x: self.logger.info(" ".join(x.split())) if x.strip() else None
            sys.stderr.write = lambda x: self.logger.error(" ".join(x.split())) if x.strip() else None
            self.redirect_streams = True

    def remove_handler(self, handler_type):
        """
        Remove handler from logger.

        :param handler_type: 'stream' or 'file'
        """
        assert handler_type in ['stream', 'file'], ('Handler type should be '
                                                    '"stream" or "file"')
        obj = {'stream': logging.StreamHandler,
               'file': logging.FileHandler}[handler_type]
        for handler in self.logger.handlers:
            if isinstance(handler, obj):
                self.logger.removeHandler(handler)
