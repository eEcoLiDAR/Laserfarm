import os
import laspy
import unittest

import numpy as np

from laserchicken import export

from laserfarm.pipeline import Pipeline
from laserfarm.pipeline_remote_data import PipelineRemoteData


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


class ShortPipelineRemoteData(PipelineRemoteData):

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


class TestDerivedPipeline(unittest.TestCase):
    # Need to be setup in setUp method by derived tests
    pipeline = None

    def setUp(self):
        self.pipeline = Pipeline()

    def test_pipelineTasksAreMethods(self):
        for task in self.pipeline.pipeline:
            self.assertTrue(hasattr(self.pipeline, task))

    def test_inputIsDict(self):
        self.assertIsInstance(self.pipeline.input, dict)

    def test_inputIsEmpty(self):
        self.assertDictEqual(self.pipeline.input, {})


class TestDerivedRemoteDataPipeline(TestDerivedPipeline):

    def setUp(self):
        self.pipeline = PipelineRemoteData()

    def test_remoteDataMethods(self):
        for task in ('setup_local_fs', 'pullremote', 'pushremote', 'cleanlocalfs'):
            self.assertTrue(hasattr(self.pipeline, task))

    def test_remoteDataAttributes(self):
        for attribute in ('input_folder', 'output_folder', 'input_path'):
            self.assertTrue(hasattr(self.pipeline, attribute))


def create_test_point_cloud(nx_values=10, grid_spacing=1., offset=0., log=True):
    np.random.seed(1234)
    x = np.linspace(0., nx_values*grid_spacing, nx_values, endpoint=False)
    xv, yv = np.meshgrid(x, x)

    x, y = np.transpose(np.column_stack((xv.flatten(), yv.flatten()))
                        + np.array(offset))
    z = np.random.uniform(-0.5, 0.5, x.size)
    feature_1 = np.zeros_like(x, dtype='int32')
    feature_2 = np.full_like(x, np.nan)
    feature_3 = np.full_like(x, 0.)

    point_cloud = {'vertex': {}}
    for name, array in zip(['x', 'y', 'z', 'feature_1', 'feature_2'],
                           [x, y, z, feature_1, feature_2, feature_3]):
        point_cloud['vertex'][name] = {'data': array,
                                       'type': array.dtype.name}
    if log:
        point_cloud.update({'log': [{'time': '2018-01-18 16:01',
                                     'module': 'load',
                                     'parameters': [],
                                     'version': '0.9.2'}]})
    return point_cloud


def write_PLY_targets(directory, indices, grid_spacing=10., nx_values=10,
                      origin=(-113107.8100, 214783.8700), feature='',
                      is_binary=False):
    cell_offset = grid_spacing * nx_values
    for (nx, ny) in indices:
        offset_x = origin[0] + nx * cell_offset
        offset_y = origin[1] + ny * cell_offset
        point_cloud = create_test_point_cloud(nx_values=nx_values,
                                              grid_spacing=grid_spacing,
                                              offset=(offset_x,
                                                      offset_y))
        if feature:
            file_name = 'tile_{}_{}_{}.ply'.format(nx, ny, feature)
            attributes = [feature]
        else:
            file_name = 'tile_{}_{}.ply'.format(nx, ny)
            attributes = 'all'
        file_path = os.path.join(directory, file_name)
        export(point_cloud, file_path, attributes=attributes,
               is_binary=is_binary)


def get_number_of_points_in_LAZ_file(filename):
    with laspy.open(filename) as f:
        count = f.header.point_count
    return count
