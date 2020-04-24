import logging
import pathlib

from lc_macro_pipeline.pipeline import Pipeline
from lc_macro_pipeline.remote_utils import get_wdclient, purge_local, \
    pull_from_remote, push_to_remote
from lc_macro_pipeline.utils import check_dir_exists


logger = logging.getLogger(__name__)


class PipelineRemoteData(Pipeline):
    """ Pipeline extension to deal with remote input/output """

    _input_folder = pathlib.Path('.')
    _output_folder = pathlib.Path('.')
    _input_path = None
    _wdclient = None

    def localfs(self, input_folder, output_folder, input_file=None):
        """
        IO setup for the local file system.

        :param input_folder: full path to input folder on local filesystem.
        :param output_folder: full path to output folder on local filesystem \
                              This folder is considered root for all output \
                              paths specified
        :param input_file: (optional) name of the input file to be retrieved
        """
        self.input_folder = input_folder
        logger.info('Input dir set to {}'.format(self.input_folder))
        if input_file is not None:
            self.input_path = input_file
            logger.info('Input path set to {}'.format(self.input_path))
        # Do not check existence of input folder as it may be retrieved from
        # remote fs
        check_dir_exists(output_folder, should_exist=True, mkdir=True)
        self.output_folder = output_folder
        logger.info('Output dir set to {}'.format(self.output_folder))
        if self.logger is not None:
            self.logger.start_log_to_file(directory=self.output_folder.as_posix())
        return self

    def setup_webdav_client(self, webdav_options):
        self._wdclient = get_wdclient(webdav_options)
        return self

    def pullremote(self, remote_origin):
        """
        pull directory with input file(s) from remote to local fs

        :param remote_origin: path to directory on remote fs
        """
        if self._wdclient is None:
            raise RuntimeError('WebDAV client not setup!')
        remote_path = pathlib.Path(remote_origin)
        local_path = self.input_path
        if self.input_path.absolute() != self.input_folder.absolute():
            remote_path = remote_path.joinpath(self.input_path.name)
            if self.input_path.suffix:
                local_path = self.input_folder
        logger.info('Pulling from WebDAV {} ...'.format(remote_path))
        pull_from_remote(self._wdclient,
                         local_path.as_posix(),
                         remote_path.as_posix())
        logger.info('... pulling completed.')
        return self

    def pushremote(self, remote_destination):
        """
        push directory with output from local fs to remote_dir

        :param remote_destination: path to remote target directory
        """
        if self._wdclient is None:
            raise RuntimeError('WebDAV client not setup!')
        logger.info('Pushing to WebDAV {} ...'.format(remote_destination))
        push_to_remote(self._wdclient,
                       self.output_folder.as_posix(),
                       remote_destination)
        logger.info('... pushing completed.')
        return self

    def cleanlocalfs(self):
        """ remove pulled input and results (after push) """
        logger.info('Removing input and output folders')
        purge_local(self.input_folder.as_posix())
        purge_local(self.output_folder.as_posix())
        return self

    def run(self, pipeline=None):
        """
        Run the (augmented) pipeline

        :param pipeline: (optional) Consider the input pipeline if provided
        """
        _pipeline = pipeline if pipeline is not None else self.pipeline
        _pipeline = ('localfs', 'pullremote') + _pipeline + ('pushremote',
                                                             'cleanlocalfs')
        super(PipelineRemoteData, self).run(pipeline=_pipeline)

    @property
    def input_folder(self):
        return self._input_folder

    @input_folder.setter
    def input_folder(self, input_folder):
        self._input_folder = pathlib.Path(input_folder)

    @property
    def output_folder(self):
        return self._output_folder

    @output_folder.setter
    def output_folder(self, output_folder):
        self._output_folder = pathlib.Path(output_folder)

    @property
    def input_path(self):
        path = self.input_folder
        if self._input_path is not None:
            path /= self._input_path
        return path

    @input_path.setter
    def input_path(self, path):
        self._input_path = pathlib.Path(path)

