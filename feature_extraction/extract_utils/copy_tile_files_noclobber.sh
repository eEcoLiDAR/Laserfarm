#!/bin/bash
#
# accepts list of file names to copy, input file path, output file path
#
xargs --arg-file=$1 \
      --replace \
      --max-procs=3 \
      --verbose \
      /bin/bash -c "cp -n $2/{}$4 $3/"

