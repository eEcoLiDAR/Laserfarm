#remote_utils.py

import pathlib
import os
import json
from webdav3.client import Client as wd3client
from webdav3.client import WebDavException
from lc_macro_pipeline.utils import check_path_exists, check_file_exists, \
    check_dir_exists, get_args_from_configfile, shell_execute_command

def get_wdclient(options=None):
    """
     get webdav

     :param options: specification of options for the client. Either as
                     path to configuration file (str) or a dict
    """
    if isinstance(options,str):
        check_file_exists(options,should_exist=True)
        options = get_options_from_file(options)
    else if isinstance(options,dict):
        pass
    else:
        raise TypeError('unrecognized type {} for client \
                         options'.format(type(options)))
    #check_options(options)
    wdclient = wd3client(options)
    return wdclient


def get_options_from_file(options):
    """
    read webdav client option from configuation file. Expects presence of
    an authentication file with user/pwd details

    :param options: filepath of configuration file
    """
    args = get_args_from_configfile(options)
    if 'authenticationfile' in args.keys():
        check_file_exists(args['authenticationfile'],should_exist=True)
        authentication = get_args_from_configfile(args['authenticationfile'])
        args.pop('authenticationfile',None)
        args.update(authentication)
    return args


def check_options(options):
    #TODO add sanity check of options
    return


def pull_file_from_remote(wdclient,remote_origin,local_destination,file):
    """
    download a file from remote store to local fs.

    :param wdclient: instance of webdav client
    :param remote_origin: parent directory path on remote fs
    :param local_destination: parent directory path on local fs
    :param file: name of file to be downloaded
    """
    if not isinstance(file,str):
        print('Expected type str but received type {}'.format(type(file)))
        raise TypeError

    remote_path_to_file = os.path.join(remote_origin,file)
    local_path_to_file = oss.path.join(local_destination,file)

    if wdclient.check(remote_path_to_file) == True:
        try:
            wdclient.download_sync(remote_path_to_file,local_path_to_file)
        except WebDavException as exception:
            print('Failed to retrieve {} from \
                                            remote origin'.format(file))
            raise
    else:
        print("remote record {} does not exist on remote host".format(remote_path_to_file))
        raise RemoteResourceNotFound(remote_path_to_file)


def push_file_to_remote(wdclient,local_origin,remote_destination,file):
    """
    upload file to remote

    :param wdclient: instance of webdav client
    :param local_origin: directory of file on local fs
    :param remote_destination: target directory of file on remote fs
    :param file: file name
    """
    if not isinstance(file,str):
        print('Expected type str but received type {}'.format(type(file)))
        raise TypeError

    remote_path_to_file = os.path.join(remote_destination,file)
    local_path_to_file = oss.path.join(local_origin,file)

    if wdclient.check(remote_destination) == True:
        try:
            wdclient.upload_sync(remote_path_to_file,local_path_to_file)
        except WebDavException as exception:
            print('Failed to upload {} to \
                                            remote destination'.format(file))
            raise
    else:
        print("remote parent directory {} does not exist ".format(remote_destination))
        raise RemoteResourceNotFound(remote_path_to_file)


def pull_directory_from_remote(wdclient,local_dir,remote_dir,mode='pull'):
    """
    pull directory from remote to local fs. This can be done as a full download
    or a (one-way) sync.

    :param wdclient: instance of webdav client
    :param local_dir: local directory to be synced or created
    :param remote_dir: remote directory to be pulled or downloaded
    :param mode: 'pull': sync directory ; 'download': download
    """
    if mode == 'pull':
        try:
            wdclient.pull(remote_directory=remote_dir,local_directory=local_dir)
        except WebDavException as exception:
            print('Failed to pull {} to \
                                    {}'.format(remote_dir,local_dir))
            raise

    else if mode == 'download'
        try:
            wdclient.download_sync(remote_dir,local_dir)
        except WebDavException as exception:
            print('Failed to download {} to \
                                    {}'.format(remote_dir,local_dir))
            raise


def push_directory_to_remote(wdclient,local_dir,remote_dir,mode='push'):
    """
    push directory from local fs to remote. This can be done as a full upload
    or a (one-way) sync.

    :param wdclient: instance of webdav client
    :param local_dir: local directory to be synced or uploaded
    :param remote_dir: remote directory to be synced or uploaded
    :param mode: 'push': sync directory ; 'upload': upload
    """
    if not os.path.isdir(local_dir):
        print('{} is not a directory'.format(local_dir))
        raise NotADirectoryError(local_dir)

    if mode == 'push':
        try:
            wdclient.push(remote_directory=remote_dir,local_directory=local_dir)
        except WebDavException as exception:
            print('Failed to push {} to \
                                    {}'.format(local_dir,remote_dir))
            raise

    else if mode == 'upload':
        try:
            wdclient.upload_sync(local_dir,remote_dir)
        except WebDavException as exception:
            print('Failed to upload {} to \
                                    {}'.format(local_dir,remote_dir))
            raise


def purge_local(local_record):
    """
    remove record from local file system

    :param local_record : full path of local_record
    """
    if not os.path.exists(local_record):
        print('local record {} does not exits'.format(local_record))
        raise FileNotFoundError(local_record)

    shell_execute_command('rm -r '+local_record)
