# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

##[Unreleased]

## [0.1.5] - 2021-11-01
### Fixed:
- Compatibility issues (mainly path and logging related) to run on Windows

## [0.1.4] - 2021-06-29
### Fixed:
- Token authentication can be now used for WebDAV remote data access

## [0.1.3] - 2021-01-04
### Fixed:
- Fix import of the GDAL library

## [0.1.2] - 2020-11-27
### Added:
- A method for setting up multiple custom features is added to the data processing pipeline. 

## [0.1.1] - 2020-06-07
### Added:
- The KDTree's cached by Laserchicken is cleared in the data_processing (optional) and classification pipelines. 

### Changed:
- Dask's futures are released as soon as finished, freeing memory of the workers

## [0.1.0] - 2020-05-25
