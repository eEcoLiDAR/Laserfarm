#!/bin/bash

#script to setup cluster for retiling
#
#

#INPUT
# $1 localkeypath
# $2 cluster config file
# $3 merge config file


. $2
. $3


echo '"REMEMBER TO CHECK THAT WEBDAV IS MOUNTED ON ALL VMS"'

#echo $LOCALKEYPATH

for s in $(seq 0 5)
do
  ssh -i $1 $USER@$BASESERVERNAME$s.$SERVEREXTENSION "/bin/bash -c \"mkdir -pv $RETILECODEPATH \""
  ssh -i $1 $USER@$BASESERVERNAME$s.$SERVEREXTENSION "/bin/bash -c \"mkdir -pv /data/local/eecolidar/rclone/tmp/mergeTemp \""
  scp -p run_merge_ahn3_vmj.sh $USER@$BASESERVERNAME$s.$SERVEREXTENSION:$RETILECODEPATH/
  scp -p run_merge_ahn3_vmj_inlist.sh $USER@$BASESERVERNAME$s.$SERVEREXTENSION:$RETILECODEPATH/
  scp -p incremental_merge.py $USER@$BASESERVERNAME$s.$SERVEREXTENSION:$RETILECODEPATH/


  #if [ -n $4 ]
  #then
  #  scp $4 $USER@$BASESERVERNAME$s.$SERVEREXTENSION:$INPUTLIST
  #fi


  if [ $s -eq 0 ]
  then
    scp -p $1 $USER@$BASESERVERNAME$s.$SERVEREXTENSION:$CLUSTERKEYPATH
    scp -p $2 $USER@$BASESERVERNAME$s.$SERVEREXTENSION:$RETILECODEPATH/
    scp -p run_merge_ahn3.sh $USER@$BASESERVERNAME$s.$SERVEREXTENSION:$RETILECODEPATH/
    scp -p run_merge_ahn3_inlist.sh $USER@$BASESERVERNAME$s.$SERVEREXTENSION:$RETILECODEPATH/
    #scp merge_downloadList.py $USER@$BASESERVERNAME$s.$SERVEREXTENSION:$DOWNLOADCODEPATH/
    scp -p $3 $USER@$BASESERVERNAME$s.$SERVEREXTENSION:$RETILECODEPATH/
  fi
done
