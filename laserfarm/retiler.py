import logging
import os
import pdal
import pylas
import json

from laserfarm.grid import Grid
from laserfarm.pipeline_remote_data import PipelineRemoteData
from laserfarm.utils import check_file_exists, check_dir_exists


logger = logging.getLogger(__name__)


class Retiler(PipelineRemoteData):
    """ Split point cloud data into smaller tiles on a regular grid. """

    def __init__(self, input_file=None, label=None):
        self.pipeline = ('set_grid', 'split_and_redistribute', 'validate')
        self.grid = Grid()
        if input_file is not None:
            self.input_path = input_file
        if label is not None:
            self.label = label

    def set_grid(self, min_x, min_y, max_x, max_y, n_tiles_side):
        """
        Setup the grid to which the input file is retiled.

        :param min_x: min x value of tiling schema
        :param min_y: max y value of tiling schema
        :param max_x: min x value of tiling schema
        :param max_y: max y value of tiling schema
        :param n_tiles_side: number of tiles along axis. Tiling MUST be square
        (enforced)
        """
        logger.info('Setting up the target grid')
        self.grid.setup(min_x, min_y, max_x, max_y, n_tiles_side)
        return self

    def split_and_redistribute(self):
        """
        Split the input file using PDAL and organize the tiles in subfolders
        using the location on the input grid as naming scheme.
        """
        self._check_input()
        logger.info('Splitting file {} with PDAL ...'.format(self.input_path))
        _run_PDAL_splitter(self.input_path, self.output_folder,
                           self.grid.grid_mins, self.grid.grid_maxs,
                           self.grid.n_tiles_side)
        logger.info('... splitting completed.')
        tiles = [f for f in self.output_folder.iterdir()
                 if (f.is_file()
                     and f.suffix.lower() == self.input_path.suffix.lower()
                     and f.stem.startswith(self.input_path.stem)
                     and f.name != self.input_path.name)]
        logger.info('Redistributing files to tiles ...')
        for tile in tiles:
            (_, tile_mins, tile_maxs, _, _) = _get_details_pc_file(str(tile))

            # Get central point to identify associated tile
            cpX = tile_mins[0] + ((tile_maxs[0] - tile_mins[0]) / 2.)
            cpY = tile_mins[1] + ((tile_maxs[1] - tile_mins[1]) / 2.)
            tile_id = _get_tile_name(*self.grid.get_tile_index(cpX, cpY))

            retiled_folder = self.output_folder.joinpath(tile_id)
            check_dir_exists(retiled_folder, should_exist=True, mkdir=True)
            logger.info('... file {} to {}'.format(tile.name, tile_id))
            tile.rename(retiled_folder.joinpath(tile.name))
        logger.info('... redistributing completed.')
        return self

    def validate(self, write_record_to_file=True):
        """
        Validate the produced output by checking consistency in the number
        of input and output points.
        """
        self._check_input()
        logger.info('Validating split ...')
        (parent_points, _, _, _, _) = _get_details_pc_file(self.input_path.as_posix())
        logger.info('... {} points in parent file'.format(parent_points))
        valid_split = False
        split_points = 0
        redistributed_to = []
        tiles = self.output_folder.glob('tile_*/{}*'.format(self.input_path.stem))

        for tile in tiles:
            if tile.is_file():
                (tile_points, _, _, _, _) = _get_details_pc_file(tile.as_posix())
                logger.info('... {} points in {}'.format(tile_points,
                                                         tile.name))
                split_points += tile_points
                redistributed_to.append(tile.parent.name)

        if parent_points == split_points:
            logger.info('... split validation completed.')
            valid_split = True
        else:
            logger.error('Number of points in parent and child tiles differ!')

        retile_record = {'file': self.input_path.as_posix(),
                         'redistributed_to': redistributed_to,
                         'validated': valid_split}

        if write_record_to_file:
            _write_record(self.input_path.stem,
                          self.output_folder,
                          retile_record)
        return self

    def _check_input(self):
        if not self.grid.is_set:
            raise ValueError('The grid has not been set!')
        check_file_exists(self.input_path, should_exist=True)
        check_dir_exists(self.output_folder, should_exist=True)


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
        logger.error('failure to open {}'.format(filename))
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
    _print_PDAL_pipeline_dict(PDAL_pipeline_dict)
    PDAL_pipeline = pdal.Pipeline(json.dumps(PDAL_pipeline_dict))
    logger.debug("... running PDAL:")
    PDAL_pipeline.execute()


def _print_PDAL_pipeline_dict(dictionary):
    logger.debug('... PDAL input:')
    for el in dictionary.get("pipeline"):
        if isinstance(el, dict):
            _dict = el.copy()
            logger.debug("... type = {}".format(_dict.pop('type')))
            for key, val in _dict.items():
                logger.debug("... {} = {}".format(key, val))
        else:
            logger.debug("... {}".format(el))
    logger.debug('... PDAL input end.')


def _write_record(input_tile, temp_folder, retile_record):
    record_file = os.path.join(temp_folder, os.path.splitext(
        input_tile)[0] + '_retile_record.js')

    with open(record_file, 'w') as recfile:
        recfile.write(json.dumps(retile_record, indent=4, sort_keys=True))
