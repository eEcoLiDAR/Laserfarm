import logging
import sys

class Logger(object):

    def __init__(self, level='debug', format=None):
        self.logger = logging.getLogger('lc_macro_pipeline')
        self.level = level
        _format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s' if format is None else format
        self.formatter = logging.Formatter(_format)

    def add_stream(self, stream='stderr'):
        assert stream in ['stdout', 'stderr'], ('Stream should be '
                                                '"stdout" or "stderr"')
        sys_stream = getattr(sys, stream)
        sh = logging.StreamHandler(sys_stream)
        sh.setFormatter(self.formatter)
        sh.setLevel(self.level)
        self.logger.addHandler(sh)

    def add_file(self, filename="log.log", redirect_stream=False):
        fh = logging.FileHandler(filename, mode='w')
        fh.setFormatter(self.formatter)
        fh.setLevel(self.level)
        self.logger.addHandler(fh)

        if redirect_stream:
            logging.warning('Removing streamHandler from logger. STDOUT/STDERR'
                            'will be now directed to {}'.format(filename))
            for hdlr in self.logger.handlers:
                if isinstance(hdlr, logging.StreamHandler):
                    self.logger.removeHandler(hdlr)

            sys.stdout.write = lambda x: self.logger.info(" ".join(x.split())) if x.strip() else None
            sys.stderr.write = lambda x: self.logger.error(" ".join(x.split())) if x.strip() else None