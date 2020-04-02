import fire

from lc_macro_pipeline.data_processing import DataProcessing
from lc_macro_pipeline.retiler import Retiler


def main():
    fire.Fire({'data_processing': DataProcessing,
               'retiling': Retiler})
