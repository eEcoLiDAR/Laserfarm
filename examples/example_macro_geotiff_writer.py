from lc_macro_pipeline.geotiff_wirter import Geotiff_wirter
from lc_macro_pipeline.macro_pipeline import MacroPipeline


if __name__ == '__main__':

    macro = MacroPipeline()
    for i in range(2):
        pipeline = Geotiff_wirter()
        pipeline.config('geotiff_writting_config/geotiff_writting_config_{}.json'.format(i))
        macro.add_task(pipeline)
    res = macro.run()
    print(res)
