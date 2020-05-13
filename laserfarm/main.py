import fire

from laserfarm.data_processing import DataProcessing
from laserfarm.retiler import Retiler
from laserfarm.geotiff_writer import GeotiffWriter
from laserfarm.classification import Classification


def main():
    fire.Fire({'data_processing': DataProcessing,
               'retiling': Retiler,
               'geotiff_writer': GeotiffWriter,
               'classification': Classification})
