import numpy as np


class Grid(object):

    def __init__(self):
        self.min_x = 0.
        self.min_y = 0.
        self.max_x = 0.
        self.max_y = 0.
        self.n_tiles_side = 1

    def setup(self, min_x, min_y, max_x, max_y, n_tiles_side):
        self.n_tiles_side = n_tiles_side
        self.min_x = min_x
        self.min_y = min_y
        self.max_x = max_x
        self.max_y = max_y
        self._check_finite_extend()

    @property
    def n_tiles_side(self):
        return self._n_tiles_side

    @n_tiles_side.setter
    def n_tiles_side(self, n_tiles_side):
        if not isinstance(n_tiles_side, int) or n_tiles_side < 1:
            raise ValueError('n_tiles_side must be int > 0! Got instead: '
                             '{}'.format(n_tiles_side))
        self._n_tiles_side = n_tiles_side

    @property
    def grid_mins(self):
        return np.array([self.min_x, self.min_y], dtype=np.float)

    @property
    def grid_maxs(self):
        return np.array([self.max_x, self.max_y], dtype=np.float)

    @property
    def grid_width(self):
        return self.grid_maxs - self.grid_mins

    @property
    def tile_width(self):
        return self.grid_width / self.n_tiles_side

    def get_tile_index(self, px, py):
        self._check_finite_extend()
        point_cart = np.array([px, py], dtype=np.float).T
        point_dir = (point_cart - self.grid_mins) / self.tile_width
        indices = np.floor(point_dir).astype('int')
        # # If point is on the edge of a tile we put in the tile with lower index
        # indices[indices == self.n_tiles_side] -= 1
        return indices

    def get_tile_bounds(self, tile_index_x, tile_index_y):
        tile_index = np.array([tile_index_x, tile_index_y], dtype=np.int)
        tile_mins = tile_index * self.tile_width + self.grid_mins
        tile_maxs = tile_mins + self.tile_width
        return tile_mins, tile_maxs

    def is_point_in_tile(self, px, py, tile_index_x, tile_index_y,
                         precision=None):
        if precision is None:
            indices = np.array([tile_index_x, tile_index_y], dtype=np.int).T
            return indices == self.get_tile_index(px, py)
        else:
            point_cart = np.array([px, py], dtype=np.float).T
            tile_mins, tile_maxs = self.get_tile_bounds(tile_index_x,
                                                        tile_index_y)
            return np.logical_and(tile_mins - point_cart > precision,
                                  point_cart - tile_maxs > precision)

    def generate_tile_mesh(self, tile_index_x, tile_index_y, tile_mesh_size):

        tile_mins, tile_maxs = self.get_tile_bounds(tile_index_x, tile_index_y)

        n_points_per_dim = self.tile_width / tile_mesh_size
        if not np.any(np.isclose(n_points_per_dim, np.rint(n_points_per_dim))):
            raise Warning('The tile width is not multiple of the chosen mesh!')

        offset = tile_mins + tile_mesh_size/2.
        x = np.arange(0., self.tile_width[0], tile_mesh_size) + offset[0]
        y = np.arange(0., self.tile_width[1], tile_mesh_size) + offset[1]
        xv, yv = np.meshgrid(x, y)
        return xv.flatten(), yv.flatten()

    def _check_finite_extend(self):
        for n_dim in range(1):
            if np.isclose(self.grid_width[n_dim], 0.):
                raise ValueError('Zero grid extend in {}!'.format('xy'[n_dim]))





