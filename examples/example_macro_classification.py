from lc_macro_pipeline.classification import Classification 
from lc_macro_pipeline.macro_pipeline import MacroPipeline

mode_test='local' # 'local' or 'ssh'

if __name__ == '__main__':

    macro = MacroPipeline()
    pipeline = Classification()
    if mode_test == 'local':
        pipeline.config('local_config/example_classification.json')
        macro.add_task(pipeline)
        macro.setup_cluster(mode='local')
    elif mode_test == 'ssh':
        pipeline.config('cluster_config/example_classification.json')
        macro.add_task(pipeline)
        macro.setup_cluster(mode ='ssh',
                            hosts = ["172.17.0.2", "172.17.0.2", "172.17.0.3"],
                            connect_options={"known_hosts": None,
                                        "username":"ubuntu", 
                                        "client_keys":"/home/ubuntu/.ssh/id_rsa"},
                            worker_options={"nthreads": 1, "nprocs":2},
                            scheduler_options={"dashboard_address": "8787"})
    res = macro.run()
    print(res)
    macro.shutdown()
