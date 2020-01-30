#!/bin/bash

#This script runs feature extraction on all VMs
#
#$1 configuration file
#$2 list file without suffix
#$3 run suffix a,b,c,etc.

for s in `seq 0 5`; do ssh -i /tmp/id_rsa ubuntu@eecolidar$s.eecolidar-nlesc.surf-hosted.nl "/bin/bash -c \"((nohup /home/ubuntu/feature_extraction_scripts/run_feature_extraction_on_VM.sh $1 $2\_$s\_$3.txt > /home/ubuntu/feature_extraction_scripts/features_set_$s\_$3.out  2>/home/ubuntu/feature_extraction_scripts/features_set_$s\_$3.err) &)\""; done ;
