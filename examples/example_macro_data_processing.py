from laserfarm import DataProcessing, MacroPipeline

mode_test = 'local'  # 'local' or 'ssh'

if __name__ == '__main__':

    macro = MacroPipeline()
    pipeline = DataProcessing(input="C_43FN1_1.LAZ", tile_index=(101, 101))
    if mode_test == 'local':
        pipeline.config(from_file='local_config/example_data_processing.json')
        macro.add_task(pipeline)
        macro.setup_cluster(mode='local', )
    elif mode_test == 'ssh':
        pipeline.config(
            from_file='cluster_config/example_data_processing.json')
        macro.add_task(pipeline)
        macro.setup_cluster(mode='ssh',
                            hosts=["172.17.0.2", "172.17.0.3"],
                            connect_options={"known_hosts": None,
                                             "username": "ubuntu",
                                             "client_keys": "/home/ubuntu/.ssh/id_rsa"},
                            worker_options={"nthreads": 1, "nprocs": 2},
                            scheduler_options={"dashboard_address": "8787"})
    macro.run()
    macro.print_outcome()
    macro.shutdown()
