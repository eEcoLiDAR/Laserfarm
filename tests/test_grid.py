from pathlib import Path
import unittest
import numpy as np
import pylas

from lc_macro_pipeline.grid import Grid

try:
    import matplotlib
    matplotlib_available = True
except ModuleNotFoundError:
    matplotlib_available = False

if matplotlib_available:
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt


class TestRealGridValid(unittest.TestCase):
    _test_dir = 'test_tmp_dir'
    _test_data_dir = 'testdata'
    _test_tile_idx = [169, 108]

    _test_file_name = 'C_41CZ2_3_valid.LAZ'
    _min_x = -113107.8100
    _min_y = 214783.8700
    _max_x = 398892.1900
    _max_y = 726783.87
    _n_tiles_sides = 256

    plot = False

    def setUp(self):
        self.grid = Grid()
        self.grid.setup(min_x=self._min_x,
                        min_y=self._min_y,
                        max_x=self._max_x,
                        max_y=self._max_y,
                        n_tiles_side=self._n_tiles_sides)
        self._test_data_path = Path(self._test_data_dir).joinpath(self._test_file_name)
        self.points = _read_points_from_file(str(self._test_data_path))

    def test_isPointInTile(self):
        x_pts, y_pts = self.points.T
        mask_valid_points = self.grid.is_point_in_tile(x_pts, y_pts,
                                                       *self._test_tile_idx)
        self.assertTrue(np.all(mask_valid_points))


class TestRealGridLowPrecision(TestRealGridValid):
    """
    The following tile has been obtained by using large scale parameters (0.1)
    in the PDAL LAS writer. Some points thus fall outside the tile boundary
    when read from the file.
    """
    _test_file_name = 'C_41CZ2_3_lowprec.LAZ'

    def test_isPointInTile(self):
        x_pts, y_pts = self.points.T
        mask_valid_points = self.grid.is_point_in_tile(x_pts, y_pts,
                                                       *self._test_tile_idx)
        if (self.plot and matplotlib_available):
            _plot_points_and_tile(self.grid,
                                  self.points[~mask_valid_points],
                                  self._test_tile_idx,
                                  self._test_data_path.with_suffix('.png').name)
        self.assertFalse(np.all(mask_valid_points))

    def test_isPointInTileWithPrecision(self):
        x_pts, y_pts = self.points.T
        mask_valid_points = self.grid.is_point_in_tile(x_pts, y_pts,
                                                       *self._test_tile_idx,
                                                       precision=0.1)
        self.assertTrue(np.all(mask_valid_points))


class TestRealGridLowPrecisionRoundedOrigin(TestRealGridValid):
    """
    The following tile has been obtained by rounding off the coordinates
    of the origin and by using the default scale parameters (0.01) in the PDAL
    LAS writer.
    """
    _test_file_name = 'C_43FN1_1.LAZ'
    _test_tile_idx = [101, 101]

    _min_x = -113108.00
    _min_y = 214784.00
    _max_x = 398892.00
    _max_y = 726784.00

    def test_isPointInTile(self):
        x_pts, y_pts = self.points.T
        mask_valid_points = self.grid.is_point_in_tile(x_pts, y_pts,
                                                       *self._test_tile_idx)
        if (self.plot and matplotlib_available):
            _plot_points_and_tile(self.grid,
                                  self.points[~mask_valid_points],
                                  self._test_tile_idx,
                                  self._test_data_path.with_suffix('.png').name)
        self.assertFalse(np.all(mask_valid_points))

    def test_isPointInTileWithPrecision(self):
        x_pts, y_pts = self.points.T
        mask_valid_points = self.grid.is_point_in_tile(x_pts, y_pts,
                                                       *self._test_tile_idx,
                                                       precision=0.01)
        self.assertTrue(np.all(mask_valid_points))


def _read_points_from_file(filename):
    file = pylas.read(filename)
    return np.column_stack((file.x, file.y))


def _plot_points_and_tile(grid, points, tile_indices, filename=None):
    """
    Plot points

    :param grid: grid object
    :param points: (Nx2) array containing X,Y coordinates of the points
    :param tile_indices: [N_x, N_y], where N_i is the integer tile index along
    dimension i
    :param filename: optional, path where to save plot
    """
    # plot points
    x_pts, y_pts = points.T
    plt.scatter(x_pts, y_pts, color='r')
    # plot tile
    tile_mins, tile_maxs = grid.get_tile_bounds(*tile_indices)
    line = np.array((tile_mins,
                     [tile_mins[0], tile_maxs[1]],
                     tile_maxs,
                     [tile_maxs[0], tile_mins[1]],
                     tile_mins))
    x, y = line.T
    plt.plot(x, y, color='k')
    # add tile label
    x_cntr, y_cntr = (tile_mins + tile_maxs) / 2.
    plt.text(x_cntr, y_cntr, '({}, {})'.format(*tile_indices),
             horizontalalignment='center',
             verticalalignment='center')
    if filename is not None:
        plt.savefig(filename, dpi=300)
    else:
        plt.show()
    plt.close(plt.figure())
