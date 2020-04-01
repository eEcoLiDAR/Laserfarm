import pathlib

import os
import pdal
import pylas
import json

from lc_macro_pipeline.grid import Grid
from lc_macro_pipeline.pipeline import Pipeline
from lc_macro_pipeline.utils import shell_execute_cmd, check_file_exists, \
    check_dir_exists
from lc_macro_pipeline.remote_utils import get_wdclient, pull_from_remote, \
    push_to_remote, purge_local


class Retiler(Pipeline):
    """ Split point cloud data into smaller tiles on a regular grid. """

    def __init__(self):
        self.pipeline = ('localfs', 'pullremote', 'tiling',
                         'split_and_redistribute', 'validate',
                         'pushremote', 'cleanlocalfs')
        self.temp_folder = None
        self.filename = None
        self.tiled_temp_folder = None
        self.grid = Grid()

    def localfs(self, input_file, input_folder, temp_folder):
        """
        IO setup for the local file system.

        :param input_file: name of input file (basename)
        :param input_folder: full path to input folder on local filesystem
        :param temp_folder: full path to temp folder on local filesystem
        :return:
        """

        input_path = pathlib.Path(input_folder)
        check_dir_exists(input_path, should_exist=True, mkdir=True)
        self.filename = input_path.joinpath(input_file)
        #Do not check existence of file here as it may need to be retrieved
        #from remote fs
        self.temp_folder = pathlib.Path(temp_folder)
        check_dir_exists(self.temp_folder, should_exist=True, mkdir=True)
        self.tiled_temp_folder = self.temp_folder.joinpath(self.filename.stem)

        if self.tiled_temp_folder.is_dir():
            print('Caution temp directory {} already exists '
                  'and may contain data.'.format(str(self.tiled_temp_folder)))
        else:
            self.tiled_temp_folder.mkdir(parents=True)
        return self

    def pullremote(self, options, remote_origin):
        """
        pull file from remote to local fs

        :param options: setup options for webdav client. Can be a filepath
        :param remote_origin: path to parent directory of file on remote fs
        """
        p=self.filename
        local_dir = p.parent.as_posix()
        fname = p.name
        remote_record = os.path.join(remote_origin,fname)
        wdclient = get_wdclient(options)
        pull_from_remote(wdclient,local_dir,remote_record)
        return self

    def pushremote(self, options, remote_destination):
        """
        push retiled directories to remote and cleaan up local fs

        :param options: setup options for webdav
        :param remote_destination: remote directory to push to
        """
        wdclient = get_wdclient(options)
        push_to_remote(wdclient, self.tiled_temp_folder.as_posix(), remote_destination)
        return self

    def cleanlocalfs(self):
        """
        remove pulled input file and results of tiling (after push)
        """
        purge_local(self.filename.as_posix())
        purge_local(self.temp_tiled_folder.as_posix())
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
        self.grid.setup(min_x, min_y, max_x, max_y, n_tiles_side)
        return self

    def split_and_redistribute(self):
        """
        Split the input file using PDAL and organize the tiles in subfolders
        using the location on the input grid as naming scheme.
        """
        check_file_exists(self.filename,should_exist=True)
        _run_PDAL_splitter(self.filename, self.tiled_temp_folder,
                           self.grid.grid_mins, self.grid.grid_maxs,
                           self.grid.n_tiles_side)
        tiles = [f for f in self.tiled_temp_folder.iterdir()
                 if (f.is_file() and f.suffix.lower() in ['.las', '.laz']
                     and f.stem.startswith(self.filename.stem))]
        for tile in tiles:
            (_, tile_mins, tile_maxs, _, _) = _get_details_pc_file(str(tile))

            # Get central point to identify associated tile
            cpX = tile_mins[0] + ((tile_maxs[0] - tile_mins[0]) / 2.)
            cpY = tile_mins[1] + ((tile_maxs[1] - tile_mins[1]) / 2.)
            tile_id = _get_tile_name(*self.grid.get_tile_index(cpX, cpY))

            retiled_folder = self.tiled_temp_folder.joinpath(tile_id)
            check_dir_exists(retiled_folder, should_exist=True, mkdir=True)
            tile.rename(retiled_folder.joinpath(tile.name))
        return self

    def validate(self, write_record_to_file=True):
        """
        Validate the produced output by checking consistency in the number
        of input and output points.
        """
        check_file_exists(self.filename, should_exist=True)
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


def _get_tile_name(x_index, y_index):
    return 'tile_{}_{}'.format(int(x_index), int(y_index))


def _run_PDAL_splitter(filename, tiled_temp_folder, tiling_mins, tiling_maxs,
                       n_tiles_side):
    length_PDAL_tile = ((tiling_maxs[0] - tiling_mins[0]) /
                        float(n_tiles_side))

    outfile_with_placeholder = "_#".join([filename.stem, filename.suffix])
    outfilepath = tiled_temp_folder.joinpath(outfile_with_placeholder)

    PDAL_pipeline_dict = {
        "pipeline": [
            filename.as_posix(),
            {
                "type": "filters.splitter",
                "origin_x": "{}".format(tiling_mins[0]),
                "origin_y": "{}".format(tiling_mins[1]),
                "length": "{}".format(length_PDAL_tile)
            },
            {
                "type": "writers.las",
                "filename": outfilepath.as_posix(),
                "forward": ["scale_x", "scale_y", "scale_z"],
                "offset_x": "auto",
                "offset_y": "auto",
                "offset_z": "auto"
            }
        ]
    }
    PDAL_pipeline = pdal.Pipeline(json.dumps(PDAL_pipeline_dict))
    PDAL_pipeline.execute()

def _write_record(input_tile, temp_folder, retile_record):
    tiled_temp_folder = os.path.join(temp_folder, os.path.splitext(
        input_tile)[0])
    record_file = os.path.join(tiled_temp_folder, os.path.splitext(
        input_tile)[0] + '_retile_record.js')

    with open(record_file, 'w') as recfile:
        recfile.write(json.dumps(retile_record, indent=4, sort_keys=True))
