#!/bin/bash

#script runs incremental retiling of doewnloaded ahn3 dataset
#on vm instance. called by run_retile_ahn3.sh




#INPUT
# $1 VM id
# $2VM id number
# $3 NUMBERVMS
# $4 RETILECODEPATH
# $5 DOWNLOADCONFIGFILE
# $6 OUTPUTDIRECTORY (Retiled)
# $7 TEMPDIRECTORY (retiled;local)
# $8 EXTENT
# $9 NUMBEROFTILES
# $10 NUMBEROFPROCESSES
# $11 PATH envitonment variable of calling script
echo '---'
VMID=$1
IDNUM=$2
NVM=$3
RTCODEPATH=$4
DLCONFIGFILE=$5
OUTDIR=$6
TEMPDIR=$7
EXT=$8
NTILES=$9
shift
NPROC=$9
shift
LPATH=$9


echo $VMID
echo $IDNUM
echo $NVM
echo $RTCODEPATH
echo $DLCONFIGFILE
echo $OUTDIR
echo $TEMPDIR
echo "$EXT"
echo $NTILES
echo $NPROC
echo $LPATH
echo '---'
echo $PATH
export PATH=$LPATH:$PATH
echo $PATH
echo '---'
#echo $9
#echo $8
#echo "$7"
#echo $6
#echo $5
#echo $4
#echo $3
#cho $2
#cho $1
#echo "reached here"
#echo '---'
#echo $0
echo '---'
echo $PATH

#echo " $3/incremental_retile.py -nvm $2 -v $1 -cf $4 -o $5 -t $6 -e "\'$7\'" -n $8 -p $9"

/bin/bash --login -c " $RTCODEPATH/incremental_retile.py -nvm $NVM -v $VMID -nid $IDNUM -cf $DLCONFIGFILE -o $OUTDIR -t $TEMPDIR -e \"$EXT\" -n $NTILES -p $NPROC ;"
#$3/incremental_retile.py -nvm $2 -v $1 -cf $4 -o $5 -t $6 -e "$7" -n $8 -p $9
