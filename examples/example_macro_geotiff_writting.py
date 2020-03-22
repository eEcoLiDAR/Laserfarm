import sys
sys.path.append('/mnt/c/Users/OuKu/Developments/eEcolidar/lcMacroPipeline')

from lc_macro_pipeline.geotiff_writer import Geotiff_writer
from lc_macro_pipeline.macro_pipeline import MacroPipeline


if __name__ == '__main__':

    macro = MacroPipeline()
    for i in range(2):
        pipeline = Geotiff_writer()
        pipeline.config('geotiff_writing_config/geotiff_writing_config_{}.json'.format(i))
        macro.add_task(pipeline)
    # macro.setup_client() # Auto setup on local machine
    macro.setup_client(mode='mannual', num_workers=2, num_threads_per_worker=2)
    res = macro.run()
    macro.shutdown_client()
    print(res)
