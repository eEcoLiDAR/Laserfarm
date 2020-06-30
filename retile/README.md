# Incremental retiling of AHN(3) data

This repository provides functionality to retile AHN(3) data to a user specified tiling
scheme in an incremental fashion. It either takes in a list of which original AHN tiles have been
recently updated (i.e downloaded) or a user supplied list of files, and retiles these.

Subsequently the retiled data can be merged to the user defined scheme.

Execution is driven by a master bash script calling individual instance scripts on each VM and executing the provided python scripts

This makes use of python routines developed on the basis of the mpc-tiling script provided by the MassivePotreeConverter tool (provided in pympc)

Utilties to provision a cluster are provided in cluster_utilities
