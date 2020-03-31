import sys
sys.path.append('/mnt/c/Users/OuKu/Developments/eEcolidar/lcMacroPipeline')

from lc_macro_pipeline.retiler import Retiler
from lc_macro_pipeline.macro_pipeline import MacroPipeline

if __name__ == '__main__':

    macro = MacroPipeline()

    pipeline = Retiler()
    pipeline.config('local_config/example_retiling_config.json') #local
    # pipeline.config('cluster_config/example_retiling_config.json') # cluster
    macro.add_task(pipeline)
    macro.setup_client(mode='local') # local test
    # macro.setup_client(mode = 'ssh',
    #                    hosts = ["172.17.0.2", "172.17.0.3"], 
    #                    connect_options={"known_hosts": None, 
    #                                       "username":"ubuntu", 
    #                                       "client_keys":"/home/ubuntu/.ssh/id_rsa"},
    #                    worker_options={"nthreads": 1, "nprocs":2}, 
    #                    scheduler_options={"dashboard_address": "8787"}) # cluster test
    res = macro.run()
    print(res)
    macro.shutdown()
