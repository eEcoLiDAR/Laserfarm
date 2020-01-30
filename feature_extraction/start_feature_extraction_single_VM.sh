#!/bin/bash

#This script starts up feature extraction on 1 VM. also copies config file as required
#
#$1 index VM
#$2 configuration file
#$3 index list file

LISTPATH=/data/local/eecolidar_webdav/01_Escience/ALS/Netherlands/ahn3_current/ahn3_current_retiled_LAZ_joined_subsetlists/ahn3cj_set__$3.txt

echo "copying config file $2"
scp -i /tmp/id_rsa $2 ubuntu@eecolidar$1.eecolidar-nlesc.surf-hosted.nl:/home/ubuntu/feature_extraction_scripts/


echo 'executing feature extraction on VM'

ssh -i /tmp/id_rsa ubuntu@eecolidar$1.eecolidar-nlesc.surf-hosted.nl "/bin/bash -c \"((nohup /home/ubuntu/feature_extraction_scripts/run_feature_extraction_on_VM.sh $2 $LISTPATH > /home/ubuntu/feature_extraction_scripts/features_set_$3\_onVM_$1.out 2>/home/ubuntu/feature_extraction_scripts/features_set_$3\_onVM_$1.err) &)\""

#for s in `seq 0 5`; do ssh -i /tmp/id_rsa ubuntu@eecolidar$s.eecolidar-nlesc.surf-hosted.nl "/bin/bash -c \"((nohup /home/ubuntu/feature_extraction_scripts/run_feature_extraction_on_VM.sh $1 $2\_$s\_$3.txt > /home/ubuntu/feature_extraction_scripts/features_set_$s\_$3.out  2>/home/ubuntu/feature_extraction_scripts/features_set_$s\_$3.err) &)\""; done ;
