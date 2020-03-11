import pathlib

import os
import numpy as np
import pylas
import json

from lc_macro_pipeline.pipeline import Pipeline
from lc_macro_pipeline.utils import shell_execute_cmd, check_file_exists, \
    check_dir_exists


class Retiler(Pipeline):
    """ Split point cloud data into smaller tiles on a regular grid. """

    def __init__(self):
        self.pipeline = ('localfs', 'tiling', 'split_and_redistribute',
                         'validate')
        self.temp_folder = None
        self.filename = None
        self.tiled_temp_folder = None
        self.tiling_mins = np.zeros(2)
        self.tiling_maxs = np.zeros(2)
        self.n_tiles_side = 0

    def localfs(self, input_file, input_folder, temp_folder):
        """
        IO setup for the local file system.

        :param input_file: name of input file (basename)
        :param input_folder: full path to input folder on local filesystem
        :param temp_folder: full path to temp folder on local filesystem
        :return:
        """

        input_path = pathlib.Path(input_folder)
        check_dir_exists(input_path, should_exist=True)
        self.filename = input_path.joinpath(input_file)
        check_file_exists(self.filename, should_exist=True)
        self.temp_folder = pathlib.Path(temp_folder)
        check_dir_exists(self.temp_folder, should_exist=True, mkdir=True)
        self.tiled_temp_folder = self.temp_folder.joinpath(self.filename.stem)

        if self.tiled_temp_folder.is_dir():
            print('Caution temp directory {} already exists '
                  'and may contain data.'.format(str(self.tiled_temp_folder)))
        else:
            self.tiled_temp_folder.mkdir(parents=True)
        return self

    def tiling(self, min_x, min_y, max_x, max_y, n_tiles_side):
        """
        Setup the grid to which the input file is retiled.

        :param min_x: min x value of tiling schema
        :param min_y: max y value of tiling schema
        :param max_x: min x value of tiling schema
        :param max_y: max y value of tiling schema
        :param n_tiles_side: number of tiles along axis. Tiling MUST be square
        (enforced)
        """
        self.tiling_mins[:] = [min_x, min_y]
        self.tiling_maxs[:] = [max_x, max_y]
        self.n_tiles_side = n_tiles_side
        return self

    def split_and_redistribute(self):
        """
        Split the input file using PDAL and organize the tiles in subfolders
        using the location on the input grid as naming scheme.
        """
        return_code, ret_message = _run_PDAL_splitter(str(self.filename),
                                                      str(self.tiled_temp_folder),
                                                      self.tiling_mins,
                                                      self.tiling_maxs,
                                                      self.n_tiles_side)
        if return_code != 0:
            raise Exception('failure in PDAL splitter: ' + ret_message)

        tiles = [f for f in self.tiled_temp_folder.iterdir()
                 if (f.is_file() and f.suffix.lower() in ['.las', '.laz']
                     and f.stem.startswith(self.filename.stem))]
        for tile in tiles:
            (_, tile_mins, tile_maxs, _, _) = _get_details_pc_file(str(tile))

            # Get central point to identify associated tile
            cpX = tile_mins[0] + ((tile_maxs[0] - tile_mins[0]) / 2.)
            cpY = tile_mins[1] + ((tile_maxs[1] - tile_mins[1]) / 2.)
            tile_id = _get_tile_name(*_get_tile_index(cpX, cpY,
                                                      self.tiling_mins,
                                                      self.tiling_maxs,
                                                      self.n_tiles_side))

            retiled_folder = self.tiled_temp_folder.joinpath(tile_id)
            check_dir_exists(retiled_folder, should_exist=True, mkdir=True)
            tile.rename(retiled_folder.joinpath(tile.name))
        return self

    def validate(self, write_record_to_file=True):
        """
        Validate the produced output by checking consistency in the number
        of input and output points.
        """
        (parent_points, _, _, _, _) = _get_details_pc_file(str(self.filename))
        valid_split = False
        split_points = 0
        redistributed_to = []
        tiles = self.tiled_temp_folder.glob('tile_*/{}*'.format(self.filename.stem))

        for tile in tiles:
            if tile.is_file():
                (tile_points, _, _, _, _) = _get_details_pc_file(str(tile))
                split_points += tile_points
                redistributed_to.append(tile.parent.name)

        if parent_points == split_points:
            valid_split = True

        retile_record = {'file': str(self.filename),
                         'redistributed_to': redistributed_to,
                         'validated': valid_split}

        if write_record_to_file:
            _write_record(self.filename.stem, self.temp_folder, retile_record)
        return self


def _get_details_pc_file(filename):
    try:
        with pylas.open(filename) as file:
            count = file.header.point_count
            mins = file.header.mins
            maxs = file.header.maxs
            scales = file.header.scales
            offsets = file.header.offsets
        return (count, mins, maxs, scales, offsets)

    except IOError:
        print('failure to open {}'.format(filename))
        return None


def _get_tile_index(pX, pY, tiling_mins, tiling_maxs, n_tiles_side):
    xpos = int((pX - tiling_mins[0]) * n_tiles_side /
               (tiling_maxs[0] - tiling_mins[0]))
    ypos = int((pY - tiling_mins[1]) * n_tiles_side /
               (tiling_maxs[1] - tiling_mins[1]))
    # If it is in the edge of the box (in the maximum side)
    # we need to put in the last tile
    if xpos == n_tiles_side:
        xpos -= 1
    if ypos == n_tiles_side:
        ypos -= 1
    return (xpos, ypos)


def _get_tile_name(x_index, y_index):
    return 'tile_{}_{}'.format(int(x_index), int(y_index))


def _run_PDAL_splitter(filename, tiled_temp_folder, tiling_mins, tiling_maxs,
                       n_tiles_side):
    length_PDAL_tile = ((tiling_maxs[0] - tiling_mins[0]) /
                        float(n_tiles_side))

    tile_cmd_PDAL = ('pdal split -i ' + filename + ' -o ' + tiled_temp_folder
                     + '/' + os.path.splitext(os.path.basename(filename))[0]
                     + '.LAZ --origin_x=' + str(tiling_mins[0])
                     + ' --origin_y=' + str(tiling_mins[1])
                     + ' --length ' + str(length_PDAL_tile))

    tile_return, tile_out_err = shell_execute_cmd(tile_cmd_PDAL)

    return tile_return, tile_out_err


def _write_record(input_tile, temp_folder, retile_record):
    tiled_temp_folder = os.path.join(temp_folder, os.path.splitext(
        input_tile)[0])
    record_file = os.path.join(tiled_temp_folder, os.path.splitext(
        input_tile)[0] + '_retile_record.js')

    with open(record_file, 'w') as recfile:
        recfile.write(json.dumps(retile_record, indent=4, sort_keys=True))
