from lc_macro_pipeline.geotiff_writer import Geotiff_writer
from lc_macro_pipeline.macro_pipeline import MacroPipeline


if __name__ == '__main__':

    macro = MacroPipeline()
    for i in range(2):
        pipeline = Geotiff_writer()
        pipeline.config('geotiff_writing_config/geotiff_writing_config_{}.json'.format(i))
        macro.add_task(pipeline)
    macro.setup_client(mode='local')
    res = macro.run()
    print(res)
    macro.shutdown()
