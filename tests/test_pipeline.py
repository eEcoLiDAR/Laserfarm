import json
import os
import shutil
import unittest

from lc_macro_pipeline.pipeline import Pipeline

from .tools import ShortPipeline

class TestPipelineObject(unittest.TestCase):

    def test_pipelineDefault(self):
        pip = Pipeline()
        self.assertTrue(isinstance(pip.pipeline, tuple))
        self.assertTrue(len(pip.pipeline) == 0)

    def test_setPipelineNotValid(self):
        pip = Pipeline()
        with self.assertRaises(TypeError):
            pip.pipeline = 0
        with self.assertRaises(AssertionError):
            pip.pipeline = ['load']

    def test_inputDefault(self):
        pip = Pipeline()
        self.assertTrue(isinstance(pip.input, dict))
        self.assertTrue(len(pip.input) == 0)

    def test_setInputNotDictionary(self):
        pip = Pipeline()
        with self.assertRaises(TypeError):
            pip.input = []
        with self.assertRaises(TypeError):
            pip.input = 'test'

    def test_setInputAttributeNotInPipeline(self):
        pip = Pipeline()
        with self.assertRaises(Warning):
            pip.input = {'test': 5}

class TestShortPipeline(unittest.TestCase):

    _test_dir = 'test_tmp_dir'

    def setUp(self):
        os.mkdir(self._test_dir)

    def tearDown(self):
        shutil.rmtree(self._test_dir)

    def test_setInput(self):
        pip = ShortPipeline()
        input = {'foo': 1, 'bar': 2}
        pip.input = input
        self.assertDictEqual(input, pip.input)

    def test_setInputExtraAttribute(self):
        pip = ShortPipeline()
        with self.assertRaises(Warning):
            pip.input = {'foo': 1, 'bar': 2, 'test': 3}
        pip = ShortPipeline()
        pip.input = {'foo': 1, 'bar': 2}
        pip.input['test'] = 3
        with self.assertRaises(Warning):
            pip.run()

    def test_setInputFromJSONConfigFile(self):
        expected_output = {'a': 1, 'b': 'test', 'c': None}
        input = {'foo': {'a': 1}, 'bar': ['test']}
        config_path = os.path.join(self._test_dir, 'input.json')
        with open(config_path, 'w') as f:
            json.dump(input, f)
        pip = ShortPipeline()
        pip.config(config_path)
        self.assertDictEqual(input, pip.input)
        pip.run()
        self.assertDictEqual(expected_output, pip.output)

    def test_runFullPipelineDifferentInput(self):
        expected_output = {'a': 1, 'b': 'test', 'c': None}
        input = {'foo': 1, 'bar': 'test'}
        self.assertDictEqual(expected_output,
                             _run_short_pipeline_and_get_output(input))
        input = {'foo': [1], 'bar': ['test']}
        self.assertDictEqual(expected_output,
                             _run_short_pipeline_and_get_output(input))
        input = {'foo': {'a': 1}, 'bar': {'b': 'test'}}
        self.assertDictEqual(expected_output,
                             _run_short_pipeline_and_get_output(input))

    def test_runPartialPipeline(self):
        expected_output = {'a': 1}
        input = {'foo': 1}
        self.assertDictEqual(expected_output,
                             _run_short_pipeline_and_get_output(input))
        input = {'foo': [1]}
        self.assertDictEqual(expected_output,
                             _run_short_pipeline_and_get_output(input))
        input = {'foo': {'a': 1}}
        self.assertDictEqual(expected_output,
                             _run_short_pipeline_and_get_output(input))


def _run_short_pipeline_and_get_output(input):
    pipeline = ShortPipeline()
    pipeline.input = input
    pipeline.run()
    return pipeline.output

