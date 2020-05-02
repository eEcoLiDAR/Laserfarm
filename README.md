# Laserchicken Macro(-ecology) Pipeline

[![Actions Status](https://github.com/eEcoLiDAR/lcMacroPipeline/workflows/build%20and%20test/badge.svg?branch=add_unit_tests)](https://github.com/eEcoLiDAR/lcMacroPipeline/actions) [![codecov](https://codecov.io/gh/eEcoLiDAR/lcMacroPipeline/branch/add_unit_tests/graph/badge.svg)](http://codecov.io/github/eEcoLiDAR/lcMacroPipeline/branch/add_unit_tests)

## Description

Laserchicken (LC) is a FOSS tool for extracting statistical properties of subgroups of points from a
(LiDAR) point cloud data set. LC itself is domain agnostic, however one of its main envisaged uses is in
large area remote-sensing, and macro-ecology in particular.

The Laserchicken Macro(-ecology) Pipeline provides a FOSS wrapper to Laserchicken supporting the use of
massive LiDAR point cloud data sets for macro-ecology, from data preparation to scheduling and execution
of distributed processing across a cluster of compute nodes.

## Installation

The package can be downloaded using `git`:
```shell script
git clone git@github.com:eEcoLiDAR/lcMacroPipeline.git
```
It requires the [PDAL](https://pdal.io) and [GDAL](https://gdal.org) libraries and the PDAL Python 
bindings. These packages are most easily installed through `conda` from the `conda-forge` channel. The 
remaining dependencies can be retrieved and installed using `pip`:
```shell script
conda install pdal python-pdal gdal -c conda-forge
cd lcMacroPipeline && pip install . 
```
Alternatively, a new environment with the package and all its dependencies can be created from the
YAML file provided:
```shell script
conda env create -f environment.yml
```
