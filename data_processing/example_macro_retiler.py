import time

from retile_core import Retiler
from macro_pipeline import MacroPipeline

if __name__ == '__main__':

    start = time.time()
    macro = MacroPipeline()

    for i in range(2):
        pipeline = Retiler()
        pipeline.config('retiling_config_{}.json'.format(i))
        macro.add_task(pipeline)

    res = macro.run()
    print(res)
    print(time.time() - start)
