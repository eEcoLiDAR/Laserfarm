import fire

from lc_macro_pipeline.data_processing import DataProcessing
from lc_macro_pipeline.retiler import Retiler
from lc_macro_pipeline.geotiff_writer import GeotiffWriter
from lc_macro_pipeline.classification import Classification


def main():
    fire.Fire({'data_processing': DataProcessing,
               'retiling': Retiler,
               'geotiff_writer': GeotiffWriter,
               'classification': Classification})
