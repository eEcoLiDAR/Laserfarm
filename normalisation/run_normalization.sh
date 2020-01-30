xargs --arg-file=$1 \
      --max-procs=2 \
      --verbose \
      --replace \
      python3 /home/ubuntu/normalisation_scripts/compute_normalization_laz.py /home/ubuntu/trial_laserchicken/laserchicken-readwrite-laz /data/local/eecolidar/rclone/tmp/raw_tiles/{}.LAZ 1.0 /data/local/eecolidar/rclone/tmp/norm_tiles/1x1m/{}_norm.LAZ;

