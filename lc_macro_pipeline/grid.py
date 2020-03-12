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
    def tiling_mins(self):
        return np.array([self.min_x, self.min_y], dtype=np.float)

    @property
    def tiling_maxs(self):
        return np.array([self.max_x, self.max_y], dtype=np.float)

    def get_tile_index(self, px, py):
        self._check_finite_extend()
        point = np.array([px, py], dtype=np.float).T
        point = ((point - self.tiling_mins) * self.n_tiles_side /
                 (self.tiling_maxs - self.tiling_mins))
        indices = np.floor(point).astype('int')
        # If it is in the edge of the box (in the maximum side)
        # we need to put in the last tile
        indices[indices == self.n_tiles_side] -= 1
        return indices

    def generate_tile_mesh(self, tile_index_x, tile_index_y, tile_mesh_size):

        tile_index = np.array([tile_index_x, tile_index_y], dtype=np.int)

        grid_width = self.tiling_maxs - self.tiling_mins
        tile_width = grid_width / self.n_tiles_side
        if not np.any(np.isclose(tile_width/tile_mesh_size,
                                 np.rint(tile_width/tile_mesh_size))):
            raise Warning('The tile width is not multiple of the chosen mesh!')

        tile_mins = tile_index * tile_width + self.tiling_mins
        
        # TODO: do I need the following????
        tile_mins[0] = np.floor(tile_mins[0])
        tile_mins[1] = np.ceil(tile_mins[1])

        offset = tile_mins + tile_mesh_size/2.
        x = np.arange(0., tile_width[0], tile_mesh_size) + offset[0]
        y = np.arange(0., tile_width[1], tile_mesh_size) + offset[1]
        xv, yv = np.meshgrid(x, y)
        return xv.flatten(), yv.flatten()

    def _check_finite_extend(self):
        delta = self.tiling_maxs - self.tiling_mins
        for n_dim in range(1):
            if np.isclose(delta[n_dim], 0.):
                raise ValueError('Zero grid extend in {}!'.format('xy'[n_dim]))





