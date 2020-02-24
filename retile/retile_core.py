import os
import numpy as np
import time
import traceback
import pylas
import subprocess
import configparser
import argparse
import json


def retrieve_stdin():
    args = argument_parser().parse_args()
    input_file = args.input_file.strip()
    return input_file, args


def argument_parser():
    parser =  argparse.ArgumentParser()
    parser.add_argument('-f','--input_file',help='name of input \
                         file (basename)',required=True)
    parser.add_argument('-i','--input_folder',help='full path to \
                         input folder on local filesystem',default=None)
    parser.add_argument('-t','--temp_folder',help='full path to\
                         temp folder on local filesystem',default=None)
    parser.add_argument('-xmin','--tiling_x_min',help='min x value \
                         of tiling schema',default=None)
    parser.add_argument('-xmax','--tiling_x_max',help='max x value \
                         of tiling schema',default=None)
    parser.add_argument('-ymin','--tiling_y_min',help='min y value \
                         of tiling schema',default=None)
    parser.add_argument('-ymax','--tiling_y_max',help='max x value \
                         of tiling schema',default=None)
    parser.add_argument('-nts', '--n_tiles_side',help='umber of tiles\
                        along axis. Tiling MUST be square (enforced)',
                        default=None)
    parser.add_argument('-cf','--configfile',help='full path to config \
                         file. If specified overrides any passed arguments\
                         (with exception of input file).')

    return parser



def get_config(args):

    if args.configfile is not None:
        configfile = args.configfile
        if os.path.isfile(configfile):
            (input_folder, temp_folder,
            tiling_mins, tiling_maxs,
            n_tiles_side) = get_tiling_config_from_file(configfile)
        else:
            raise Exception('config file expected but not found')
    else :
        if ((args.input_folder == None) or (args.temp_folder == None)
           or (args.tiling_x_min == None) or (args.tiling_x_max == None)
           or (args.tiling_y_min == None) or (args.tiling_y_max == None)
           or (args.n_tiles_side == None)):
            raise Exception('No config file AND not all required arguments\
                            found')
        else:
            (input_folder, temp_folder,
            tiling_mins, tiling_maxs,
            n_tiles_side) = get_tiling_config_from_cl(args)

    return (input_folder, temp_folder, tiling_mins, tiling_maxs,
            n_tiles_side)



def get_tiling_config_from_cl(args):
    input_folder = args.input_folder.strip()
    temp_folder = args.temp_folder.strip()
    min_x = np.float(args.tiling_x_min.strip())
    max_x = np.float(args.tiling_x_max.strip())
    min_y = np.float(args.tiling_y_min.strip())
    max_y = np.float(args.tiling_y_max.strip())
    n_tiles_side = np.float(args.n_tiles_side.strip())
    tiling_mins = np.array([min_x,min_y])
    tiling_maxs = np.array([max_x, max_y])

    return (input_folder, temp_folder, tiling_mins, tiling_maxs,
            n_tiles_side)


def get_tiling_config_from_file(tiling_config_file):
    config = configparser.ConfigParser()
    config.read(tiling_config_file)

    #read tiling configuration
    min_x = np.float(config['TILING']['min_x'].strip())
    max_x = np.float(config['TILING']['max_x'].strip())
    min_y = np.float(config['TILING']['min_y'].strip())
    max_y = np.float(config['TILING']['max_y'].strip())
    n_tiles_side = np.float(config['TILING']['n_tiles_side'].strip())
    tiling_mins = np.array([min_x,min_y])
    tiling_maxs = np.array([max_x, max_y])

    #read local (node) filesystem configuration
    input_folder = config['LOCALFS']['input_folder'].strip()
    temp_folder = config['LOCALFS']['temp_folder'].strip()

    return (input_folder, temp_folder, tiling_mins, tiling_maxs,
            n_tiles_side)





def shell_execute_cmd(command, verbacious = False):
    """ Execute command in the SHELL. Optionally display
        stdout and stderr.
    """
    if verbacious == True:
        print(command)
    proc = subprocess.Popen(command, shell = True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    (out,err) = proc.communicate()
    out_err = '\n'.join((out.decode("utf-8") , err.decode("utf-8")))
    rcode = proc.returncode
    if verbacious == True:
        print(out_err)
    return rcode, out_err


def get_details_pc_file(filename):
    try:
        with pylas.open(filename) as file :
            count = file.header.point_count
            mins = file.header.mins
            maxs = file.header.maxs
            scales = file.header.scales
            offsets = file.header.offsets
        return (count, mins, maxs, scales, offsets)

    except IOError :
        print('failure to open {}'.format(filename))
        return None


def get_tile_index(pX, pY, tiling_mins, tiling_maxs, n_tiles_side):
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


def get_tile_name(x_index, y_index):
    return 'tile_{}_{}'.format(int(x_index), int(y_index))


def run_PDAL_splitter(filename, tiled_temp_folder, tiling_mins,
                   tiling_maxs, n_tiles_side):
    length_PDAL_tile = ((tiling_maxs[0] - tiling_mins[0])/
                        float(n_tiles_side))

    tile_cmd_PDAL = ('pdal split -i '+filename+' -o '+tiled_temp_folder
                     +'/'+os.path.splitext(os.path.basename(filename))[0]
                     +'.LAZ --origin_x='+str(tiling_mins[0])
                     +' --origin_y='+str(tiling_mins[1])
                     +' --length '+str(length_PDAL_tile))

    tile_return,tile_out_err = shell_execute_cmd(tile_cmd_PDAL)

    return tile_return,tile_out_err



def redistribute_and_validate(filename, tiled_temp_folder, tiling_mins,
                              tiling_maxs, n_tiles_side):

    (parent_points, _, _, _, _) = get_details_pc_file(filename)

    valid_split = False
    split_points = 0
    redistributed_to = []
    tiles = os.listdir(tiled_temp_folder)
    for tile in tiles:
        tile_path = os.path.join(tiled_temp_folder,tile)
        if os.path.isfile(tile_path) == True:
            (tile_points,
             tile_mins,
             tile_maxs,
              _, _) = get_details_pc_file(tile_path)

            split_points += tile_points

            # Get central point to identify associated tile
            cpX = tile_mins[0] + ((tile_maxs[0] - tile_mins[0])/2.)
            cpY = tile_mins[1] + ((tile_maxs[1] - tile_mins[1])/2.)
            tile_id = get_tile_name(*get_tile_index(cpX,cpY,tiling_mins,
                                    tiling_maxs,n_tiles_side))
            redistributed_to.append(tile_id)

            retiled_folder = os.path.join(tiled_temp_folder,tile_id)
            if not os.path.isdir(retiled_folder):
                _, _ = shell_execute_cmd('mkdir -p ' + retiled_folder)

            retiled_path = os.path.join(retiled_folder,tile)
            cmd_mv_split = 'mv ' + tile_path + ' ' + retiled_path
            r_mv, _ = shell_execute_cmd(cmd_mv_split)

    if parent_points == split_points:
        valid_split = True

    return valid_split, redistributed_to


def retile_single(input_tile, input_folder, temp_folder,
                  tiling_mins, tiling_maxs, n_tiles_side,write_record_to_file=True):
    filename = os.path.join(input_folder,input_tile)
    #check folders and data
    if os.path.isfile(filename) == False:
        raise Exception('specified input file does not exist')
    tiled_temp_folder = os.path.join(temp_folder, os.path.splitext(
                                     input_tile)[0])
    if os.path.isdir(tiled_temp_folder) == True :
        print('Caution temp directory {} already exists and may \
               contain data.'.format(tiled_temp_folder))
    else:
        _, _ = shell_execute_cmd('mkdir -p ' + tiled_temp_folder)

    return_code, ret_message = run_PDAL_splitter(filename,
                                                 tiled_temp_folder,
                                                 tiling_mins,
                                                 tiling_maxs,
                                                 n_tiles_side)
    if return_code != 0:
        raise Exception('failure in PDAL splitter: ' + ret_message)

    validated, redistributed_list = redistribute_and_validate(
                                                        filename,
                                                        tiled_temp_folder,
                                                        tiling_mins,
                                                        tiling_maxs,
                                                        n_tiles_side)
    retile_record = {'file':input_tile,
                     'redistributed_to':redistributed_list,
                     'validated':validated}
    if write_record_to_file==True:
        write_record(input_tile, temp_folder, retile_record)

    return retile_record

def write_record(input_tile, temp_folder, retile_record):
    tiled_temp_folder = os.path.join(temp_folder, os.path.splitext(
                                     input_tile)[0])
    record_file = os.path.join(tiled_temp_folder,os.path.splitext(
                                     input_tile)[0]+'_retile_record.js')
    with open(record_file,'w') as recfile:
        recfile.write(json.dumps(retile_record,indent=4,sort_keys=True))



def main():

    input_file, args = retrieve_stdin()
    (input_folder, temp_folder, tiling_mins,
     tiling_maxs, n_tiles_side) = get_config(args)

    if os.path.isdir(input_folder) == False:
        raise Exception('input path is not a valid directory.')
    if os.path.isdir(temp_folder) == False:
        print('temp folder does not exist... creating')
        _, _ = shell_execute_cmd('mkdir -p ' + temp_folder)

    retile_record = retile_single(input_file, input_folder, temp_folder,
                      tiling_mins, tiling_maxs, n_tiles_side)


if __name__ == '__main__':
    main()
