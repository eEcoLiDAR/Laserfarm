# Laserfarm
Laserfarm provides a FOSS wrapper to [Laserchicken](https://github.com/eEcoLiDAR/laserchicken) supporting the use of
massive LiDAR point cloud data sets for macro-ecology, from data preparation to scheduling and execution
of distributed processing across a cluster of compute nodes.

## Branch 0.0.0
The 0.0.0 branch constitutes the departure point for the full development of Laserfarm as a python package. The initial version of the framework (alternatively also referred to as the Laserchicken macro-ecology pipeline), constituted by the code and scripts in this 0.0.0 branch of the Laserfarm repository, is fully functional and forms the basis for the subsequent development and release versions.

However, this initial version makes a number of assumptions and comes with restrictions that have been dropped in further development.
The framework is assumed to run on a set of (independent) virtual machines to which the user has SSH access and on which they have super user privileges. These are provisioned with appropriate SSH keys and software by the user prior to any processing.
Individual steps of the pipeline are then executed using `bash` wrappers, and particularly the `xarg` built-in to pass argument lists.
These steps also rely on the user managing data availability, i.e. transferring data from remote to local storage prior to execution for some stages.

A full run of the pipeline encompases:

(1) retiling
(2) target generation
(3) normalisation
(4) feature extraction
(5) GeoTiff creation

With the scripts for each steps being provided in the corresponding sub-directories.
