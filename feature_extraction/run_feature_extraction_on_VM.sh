#!/bin/bash
#
# This script reads feature extraction configuration from input file
# executes feature extraction for a list of tiles on a VM
#
#INPUTS:
# $1 configuration file
# $2 tile list

#source configuration file
source $1


if [ "$doFilter" = True ]; then

    xargs --arg-file=$2 \
	  --replace \
          --max-procs=$numberOfProcesses \
          --verbose \
	  /bin/bash -c "[ -f $outputDirectory/{}.ply ] && echo \"File {}.ply already exists\" || echo \"Creating file {}.ply\"; python3 $featureScript -p $pathOfLaserchicken -i $inputDirectory/{}_norm.LAZ -t $targetDirectory/{}_targets.laz -r $radius -o $outputDirectory/{}.ply -fa $filterField -fv $filterValue ; "

    
else

    xargs --arg-file=$2 \
          --max-procs=$numberOfProcesses \
          --verbose \
          --replace \
	  /bin/bash -c "[ -f $outputDirectory/{}.ply ] && echo \"File {}.ply already exists\" || echo \"Creating file {}.ply\"; python3 $featureScript -p $pathOfLaserchicken -i $inputDirectory/{}_norm.LAZ -t $targetDirectory/{}_targets.laz -r $radius -o $outputDirectory/{}.ply ; " 

fi

