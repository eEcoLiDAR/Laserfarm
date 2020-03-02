import json
import pathlib


def check_path_exists(path, should_exist):
    if isinstance(path, str):
        p = pathlib.Path(path)
    elif isinstance(path, pathlib.Path):
        p = path
    else:
        raise TypeError('Unexpected type {} for input '
                        'path: {}'.format(type(path), path))

    if p.exists():
        if not should_exist:
            raise FileExistsError('Path {} exists!'.format(str(p)))
    else:
        if should_exist:
            raise FileNotFoundError('Path {} does not exists!'.format(str(p)))


def get_args_from_configfile(path):
    p = pathlib.Path(path)
    check_path_exists(p, should_exist=True)
    if p.suffix == '.json':
        with open(p.absolute()) as f:
            args = json.load(f)
    return args
