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
