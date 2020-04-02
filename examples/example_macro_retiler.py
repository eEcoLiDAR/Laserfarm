from lc_macro_pipeline.retiler import Retiler
from lc_macro_pipeline.macro_pipeline import MacroPipeline

if __name__ == '__main__':

    macro = MacroPipeline()

    for i in range(2):
        pipeline = Retiler()
        pipeline.config('retiling_config.json')
        macro.add_task(pipeline)

    res = macro.run()
    print(res)
