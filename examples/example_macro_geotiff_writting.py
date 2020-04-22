from lc_macro_pipeline.geotiff_writer import GeotiffWriter
from lc_macro_pipeline.macro_pipeline import MacroPipeline

mode_test='ssh' # 'local' or 'ssh'

if __name__ == '__main__':

    macro = MacroPipeline()
    pipeline = GeotiffWriter()
    if mode_test == 'local':
        for i in range(2):
            pipeline.config('local_config/geotiff_writing_config_{}.json'.format(i))
            macro.add_task(pipeline)
            macro.setup_client(mode='local')
    elif mode_test == 'ssh':
        for i in range(2):
            pipeline.config('cluster_config/geotiff_writing_config_{}.json'.format(i))
            macro.add_task(pipeline)
        macro.setup_client(mode = 'ssh',
                    hosts = ["172.17.0.2", "172.17.0.2", "172.17.0.3"], 
                    connect_options={"known_hosts": None, 
                                        "username":"ubuntu", 
                                        "client_keys":"/home/ubuntu/.ssh/id_rsa"},
                    worker_options={"nthreads": 1, "nprocs":2}, 
                    scheduler_options={"dashboard_address": "8787"})
    res = macro.run()
    print(res)
    macro.shutdown()
