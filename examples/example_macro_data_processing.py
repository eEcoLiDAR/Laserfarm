from lc_macro_pipeline.data_processing import DataProcessing
from lc_macro_pipeline.macro_pipeline import MacroPipeline

if __name__ == '__main__':

    macro = MacroPipeline()

    for i in range(2):
        pipeline = DataProcessing()
        pipeline.config('data_processing.json'.format(i))
        macro.add_task(pipeline)

    res = macro.run()
    print(res)
