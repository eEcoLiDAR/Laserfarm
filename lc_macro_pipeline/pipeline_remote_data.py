import pathlib

from lc_macro_pipeline.pipeline import Pipeline
from lc_macro_pipeline.remote_utils import get_wdclient, purge_local, \
    pull_from_remote, push_to_remote
from lc_macro_pipeline.utils import check_dir_exists


class PipelineRemoteData(Pipeline):
    """ Pipeline extension to deal with remote input/output """

    input_folder = None
    output_folder = None

    def localfs(self, input_folder, output_folder):
        """
        IO setup for the local file system.

        :param input_folder: full path to input folder on local filesystem.
        :param output_folder: full path to output folder on local filesystem \
                              This folder is considered root for all output \
                              paths specified
        :return:
        """
        self.input_folder = pathlib.Path(input_folder)
        # Do not check existence of input folder as it may be retrieved from
        # remote fs
        check_dir_exists(output_folder, should_exist=True, mkdir=True)
        self.output_folder = pathlib.Path(output_folder)
        filename = self.output_folder.joinpath(self.__class__.__name__).with_suffix('.log')
        self.logger.add_file(filename.as_posix(), redirect_streams=True)
        return self

    def pullremote(self, options, remote_origin):
        """
        pull directory with input file(s) from remote to local fs

        :param options: setup options for webdav client. Can be a filepath
        :param remote_origin: path to directory on remote fs
        """
        wdclient = get_wdclient(options)
        pull_from_remote(wdclient, self.input_folder.as_posix(), remote_origin)
        return self

    def pushremote(self, options, remote_destination):
        """
        push directory with output from local fs to remote_dir

        :param options: setup options for webdavclient. Can be filepath
        :param remote_destination: path to remote target directory
        """
        wdclient = get_wdclient(options)
        push_to_remote(wdclient, self.output_folder.as_posix(),
                       remote_destination)
        return self

    def cleanlocalfs(self):
        """ remove pulled input and results (after push) """
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
