import numpy as np
import logging


logger = logging.getLogger(__name__)


class Grid(object):
    """
    Class to manage the retiling of large-scale point-cloud data to a regular
    grid. Tools allow to verify whether points belong to a given tile, and to
    generate target points for feature extraction
    """
    def __init__(self):
        self.min_x = 0.
        self.min_y = 0.
        self.max_x = 0.
        self.max_y = 0.
        self.n_tiles_side = 1
        self.is_set = False

    def setup(self, min_x, min_y, max_x, max_y, n_tiles_side):
        """
        Setup the grid.

        :param min_x: Min x value of the tiling schema
        :param min_y: Min y value of the tiling schema
        :param max_x: Max x value of the tiling schema
        :param max_y: Max y value of the tiling schema
        :param n_tiles_side: Number of tiles along X and Y (tiling MUST be
        square)
        """
        self.n_tiles_side = n_tiles_side
        self.min_x = min_x
        self.min_y = min_y
        self.max_x = max_x
        self.max_y = max_y
        self._check_finite_extent()
        self._check_grid_is_square()
        self.is_set = True

    @property
    def n_tiles_side(self):
        """ Number of tiles along each direction. """
        return self._n_tiles_side

    @n_tiles_side.setter
    def n_tiles_side(self, n_tiles_side):
        if not isinstance(n_tiles_side, int) or n_tiles_side < 1:
            raise ValueError('n_tiles_side must be int > 0! Got instead: '
                             '{}'.format(n_tiles_side))
        self._n_tiles_side = n_tiles_side

    @property
    def grid_mins(self):
        """ Lower grid boundaries. """
        return np.array([self.min_x, self.min_y], dtype=np.float)

    @property
    def grid_maxs(self):
        """ Upper grid boundaries. """
        return np.array([self.max_x, self.max_y], dtype=np.float)

    @property
    def grid_width(self):
        """ Width of the grid. """
        return self.grid_maxs - self.grid_mins

    @property
    def tile_width(self):
        """ Width of a tile. """
        return self.grid_width / self.n_tiles_side

    def get_tile_index(self, px, py):
        """
        Determine the indices of the tile to which one of more points belong.

        :param px: X coordinate(s) of the point(s)
        :param py: Y coordinate(s) of the point(s)
        """
        self._check_finite_extent()
        point_cart = np.array([px, py], dtype=np.float).T
        point_dir = (point_cart - self.grid_mins) / self.tile_width
        indices = np.floor(point_dir).astype('int')
        # If point falls outside the edge of the grid raise warning
        mask_invalid_indices = np.logical_or(indices >= self.n_tiles_side,
                                             indices < 0)
        if mask_invalid_indices.any():
            # axis = 1 if len(mask_invalid_indices.shape) > 1 else 0
            # num_invalid_points = np.all(mask_invalid_indices, axis=axis).sum()
            logger.warning("Points fall outside the bounds Min X={} Y={}, "
                           "Max X={} Y={}".format(*self.grid_mins,
                                                  *self.grid_maxs))
        return indices

    def get_tile_bounds(self, tile_index_x, tile_index_y):
        """
        Determine the boundaries of a tile given its X and Y indices.

        :param tile_index_x: Tile index along X
        :param tile_index_y: Tile index along Y
        """
        tile_index = np.array([tile_index_x, tile_index_y], dtype=np.int)
        tile_mins = tile_index * self.tile_width + self.grid_mins
        tile_maxs = tile_mins + self.tile_width
        return tile_mins, tile_maxs

    def is_point_in_tile(self, px, py, tile_index_x, tile_index_y,
                         precision=None):
        """
        Determine whether one or more points belong to a tile (within an
        optional precision threshold).

        :param px: X coordinate(s) of the point(s)
        :param py: Y coordinate(s) of the point(s)
        :param tile_index_x: Tile index along X
        :param tile_index_y: Tile index along Y
        :param precision: Optional precision threshold to determine whether
        the point(s) belong to the tile
        """
        if precision is None:
            indices = np.array([tile_index_x, tile_index_y], dtype=np.int).T
            mask = indices == self.get_tile_index(px, py)
        else:
            point_cart = np.array([px, py], dtype=np.float).T
            tile_mins, tile_maxs = self.get_tile_bounds(tile_index_x,
                                                        tile_index_y)
            mask = np.logical_and(tile_mins - point_cart <= precision,
                                  point_cart - tile_maxs <= precision)
        return np.all(mask, axis=1)

    def generate_tile_mesh(self, tile_index_x, tile_index_y, tile_mesh_size):
        """
        Generate a mesh of points with a given spacing in a tile.

        :param tile_index_x: Tile index along X
        :param tile_index_y: Tile index along Y
        :param tile_mesh_size: Spacing of the mesh (NOTE: each tile should
        fit an integer number of this value)
        :return:
        """

        tile_mins, tile_maxs = self.get_tile_bounds(tile_index_x, tile_index_y)

        n_points_per_dim = self.tile_width / tile_mesh_size
        if not np.any(np.isclose(n_points_per_dim, np.rint(n_points_per_dim))):
            raise ValueError('The tile width is not multiple'
                             'of the chosen mesh!')

        offset = tile_mins + tile_mesh_size/2.
        x = np.arange(0., self.tile_width[0], tile_mesh_size) + offset[0]
        y = np.arange(0., self.tile_width[1], tile_mesh_size) + offset[1]
        xv, yv = np.meshgrid(x, y)
        return xv.flatten(), yv.flatten()

    def _check_finite_extent(self):
        for n_dim in range(1):
            if np.isclose(self.grid_width[n_dim], 0.):
                raise ValueError('Zero grid extend in {}!'.format('xy'[n_dim]))

    def _check_grid_is_square(self):
        if not np.isclose(self.tile_width[0], self.tile_width[1]):
            raise ValueError('Grid is not square!')
