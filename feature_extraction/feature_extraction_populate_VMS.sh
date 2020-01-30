#!/bin/bash

#$1 template file

for s in `seq 0 5`; do scp -i ~/.ssh/id_rsa extract_features_target_cells.py run_feature_extraction_on_VM.sh $1 ubuntu@eecolidar$s.eecolidar-nlesc.surf-hosted.nl:/home/ubuntu/feature_extraction_scripts/; done

scp -i ~/.ssh/id_rsa start_feature_extraction_on_VMs.sh ubuntu@eecolidar0.eecolidar-nlesc.surf-hosted.nl:/home/ubuntu/feature_extraction_scripts/
