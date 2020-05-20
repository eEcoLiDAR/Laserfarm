import json
import logging
import pathlib
import subprocess

logger = logging.getLogger(__name__)


def check_path_exists(path, should_exist):
    p = _string_to_path(path)
    if p.exists() and not should_exist:
        raise FileExistsError('Path {} already exists!'.format(str(p)))
    elif not p.exists() and should_exist:
        raise FileNotFoundError('Path {} does not exists!'.format(str(p)))


def check_file_exists(path, should_exist):
    p = _string_to_path(path)
    check_path_exists(p, should_exist)
    if should_exist and not p.is_file():
        raise IOError('Path {} is not a file!'.format(str(p)))


def check_dir_exists(path, should_exist, mkdir=False):
    p = _string_to_path(path)
    try:
        check_path_exists(p, should_exist)
    except FileNotFoundError:
        if mkdir:
            p.mkdir(parents=True, exist_ok=True)
        else:
            raise
    if should_exist and not p.is_dir():
        raise NotADirectoryError('Path {} is not a directory!'.format(str(p)))


def _string_to_path(path):
    if isinstance(path, str):
        p = pathlib.Path(path)
    elif isinstance(path, pathlib.Path):
        p = path
    else:
        raise TypeError('Unexpected type {} for input '
                        'path: {}'.format(type(path), path))
    return p


def get_args_from_configfile(path):
    p = pathlib.Path(path)
    check_path_exists(p, should_exist=True)
    if p.suffix == '.json':
        with open(p.absolute()) as f:
            args = json.load(f)
    else:
        raise NotImplementedError('Parser for {} file '
                                  'not implemented'.format(p.suffix))
    return args


def shell_execute_cmd(command, verbacious=False):
    """ Execute command in the SHELL. Optionally display stdout and stderr. """
    if verbacious:
        logger.info(command)
    proc = subprocess.Popen(command, shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    (out, err) = proc.communicate()
    out_err = '\n'.join((out.decode("utf-8"), err.decode("utf-8")))
    rcode = proc.returncode
    if verbacious:
        logger.info(out_err)
    return rcode, out_err


class DictToObj(object):
    def __init__(self, dictionary):
        for key, value in dictionary.items():
            setattr(self, key, value)

