import time

from data_processing import DataProcessing
from macro_pipeline import MacroPipeline

if __name__ == '__main__':

    start = time.time()
    macro = MacroPipeline()

    for i in range(2):
        pipeline = DataProcessing()
        pipeline.config('data_processing_{}.json'.format(i))
        macro.add_task(pipeline)

    res = macro.run()
    print(res)
    print(time.time() - start)
