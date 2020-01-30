#!/bin/bash

#This script is executed on 'master' server eecolidar0 and runs retiling of new downloaded files accross all VMs

#inputs
# $1 merginng configuration file
# $2 cluster configuration file

#source configuration files
. $1
. $2

NUMBERVMS=${#VMIDS[@]}

#vm_max=$(($NUMBERVMS -1))
#!!!!!!!!! this is a fix to allow the scripts to work as is without making use of eecolidar0 which currently (20190912) has issues with pdal and laszip (at least)
#vmsinuse=$vm_max

IDNUM=0
for s in "${VMIDS[@]}"
do
  echo "running merging on server $s"
  echo "making use of input file $INPUTLIST"
  echo "input directory is $INPUTDIR"
  echo "ID number is $IDNUM"

  #echo " extent is $EXTENT "
  echo $PATH
  #echo "---"
  LP=$PATH
  #ssh -i $CLUSTERKEYPATH $USER@$BASESERVERNAME$s.$SERVEREXTENSION "/bin/bash -c \"nohup $RETILECODEPATH/run_retile_ahn3_vmj.sh $s $NUMBERVMS $RETILECODEPATH $DOWNLOADCONFIGFILE $OUTPUTDIRECTORY $TEMPDIRECTORY "$EXTENT" $NUMBEROFTILES $NUMBEROFPROCESSES > $RETILECODEPATH/rtahn3_VM$s.out & \""
  #ssh -i $CLUSTERKEYPATH $USER@$BASESERVERNAME$s.$SERVEREXTENSION "/bin/bash --login -c \" echo $PATH \""
  ssh -i $CLUSTERKEYPATH $USER@$BASESERVERNAME$s.$SERVEREXTENSION "/bin/bash --login -c \" nohup $RETILECODEPATH/run_merge_ahn3_vmj_inlist.sh $s $IDNUM $NUMBERVMS $RETILECODEPATH $INPUTLIST $INPUTDIR $OUTPUTDIRECTORY $TEMPDIRECTORY $NUMBEROFPROCESSES "$LP" > $RETILECODEPATH/mahn3_VM$s.out & \""

  IDNUM=$((IDNUM +1))

done


#for s in $(seq 1 $vm_max)
#do
#  echo "running retiling on server $s"
#  echo "making use of download config file $DOWNLOADCONFIGFILE"

  #echo " extent is $EXTENT "
#  echo $PATH
  #echo "---"
#  LP=$PATH
  #ssh -i $CLUSTERKEYPATH $USER@$BASESERVERNAME$s.$SERVEREXTENSION "/bin/bash -c \"nohup $RETILECODEPATH/run_retile_ahn3_vmj.sh $s $NUMBERVMS $RETILECODEPATH $DOWNLOADCONFIGFILE $OUTPUTDIRECTORY $TEMPDIRECTORY "$EXTENT" $NUMBEROFTILES $NUMBEROFPROCESSES > $RETILECODEPATH/rtahn3_VM$s.out & \""
#  ssh -i $CLUSTERKEYPATH $USER@$BASESERVERNAME$s.$SERVEREXTENSION "/bin/bash --login -c \" echo $PATH \""
#  ssh -i $CLUSTERKEYPATH $USER@$BASESERVERNAME$s.$SERVEREXTENSION "/bin/bash --login -c \" nohup $RETILECODEPATH/run_retile_ahn3_vmj.sh $s $vmsinuse $RETILECODEPATH $DOWNLOADCONFIGFILE $OUTPUTDIRECTORY $TEMPDIRECTORY "$EXTENT" $NUMBEROFTILES $NUMBEROFPROCESSES "$LP" \""

#done


  # $4 DOWNLOADCONFIGFILE
  # $5 OUTPUTDIRECTORY (Retiled)
  # $6 TEMPDIRECTORY (retiled;local)
  # $7 EXTENT
  # $8 NUMBEROFTILES
  # $9 NUMBEROFPROCESSES
