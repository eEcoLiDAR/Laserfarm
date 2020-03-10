import os

from lc_macro_pipeline.pipeline import Pipeline


class ShortPipeline(Pipeline):

    def __init__(self):
        self.pipeline = ['foo', 'bar']
        self.output = dict()

    def foo(self, a):
        self.output['a'] = a
        return self

    def bar(self, b, c=None):
        self.output['b'] = b
        self.output['c'] = c
        return self


class ShortIOPipeline(Pipeline):

    def __init__(self):
        self.pipeline = ['open', 'write', 'close']
        self.fd = None

    def open(self, path):
        if os.path.isdir(path):
            raise IsADirectoryError('path is dir')
        self.fd = open(path, 'w')
        return self

    def write(self, text):
        self.fd.write(text)
        return self

    def close(self):
        self.fd.close()
        return self
