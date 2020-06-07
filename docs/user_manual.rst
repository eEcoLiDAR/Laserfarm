User Manual
===========

Laserfarm provides a set of tools to process massive LiDAR point-cloud data sets. In order to tackle volumes
of data such as the ones produced by airborne laser scanning, Laserfarm makes use of a *divide-et-impera*
strategy. First, the raw data is split into tiles whose size is manageable by standard computing infrastructures.
Then, point-cloud properties ("features") are extracted from the re-tiled data using the cells of a second user-defined
grid as ensembles for the statistical averaging. The mesh size of this second finer grid ultimately defines the
resolution (i.e. the pixel size) of the raster data. Finally, the properties computed for each sub-cell are exported in
a raster format and all tile contributions merged to form a single output GeoTIFF file, ideally covering the same area
as the initial raw data.

The full processing pipeline can be thus subdivided into three pipeline steps: the raw data re-tiling, the point-cloud
data processing and the raster-data merging and export. Each of these tasks can be run in a Python script that imports
the dedicated Laserfarm module or through the command line tool ``laserfarm``. The following sections
describes the main features of each of these pipelines and their general use. User interested in more advanced features
and in how these tasks can be implemented for large-scale macro-ecology calculations can have a look at the Jupyter
notebooks provided (see :ref:`Examples`).

.. _Retiling:

Raw Data Re-tiling
------------------

Point-cloud data from airborne laser scanning are typically provided as compressed files in LAZ format. The following
script illustrates a minimal example in which a data set provided as a LAZ file (``point_cloud.LAZ``) is re-tiled
according to a regular grid:

.. code-block:: python

    from laserfarm import Retiler

    pipeline = Retiler(input_file="point_cloud.LAZ")
    input_dict = {
        'set_grid': {
            'min_x': -113107.81,
            'max_x': 398892.19,
            'min_y': 214783.87,
            'max_y': 726783.87,
            'n_tiles_side': 256
        },
        'split_and_redistribute': {},
        'validate': {}
    }
    pipeline.config(input_dict)
    pipeline.run()

The re-tiling pipeline is configured using a dictionary, whose keys identify the pipeline sub-tasks that need to be
run and the values the corresponding arguments.

.. NOTE::
    The input dictionary elements can be provided in any order. The order in which the tasks are executed is defined in
    a dedicated list (the ``pipeline`` attribute of the ``Retiler`` class), which can be inspected using the
    command-line tool: ``laserfarm retiling pipeline``.

Alternatively, if the input dictionary is serialized to JSON format and stored in a file, the pipeline can be directly
configured in the following way:

.. code-block:: python

    pipeline.config(from_file="retiling_config.json")

The example above entails three sub-tasks: setting up the grid, running the splitting and validating the result.
In the first sub-task, the grid is defined using its bounding box and the number of tiles in each direction (a square
re-tiling scheme is assumed). In the following sub-task (which takes no input argument) the splitting is carried out
and the sub-sets are saved in LAZ format. The output is organized in subdirectories that are labeled with two integer
numbers corresponding to the ``X`` and ``Y`` indices in the chosen tiling scheme (each index run from ``0`` to
``n_tiles_side - 1``). Note that the `PDAL library`_ is employed for the splitting, thus taking advantage of a low-level
(C++) implementation. Finally, the result of the splitting is validated by checking that the generated LAZ files contain
the same number of points as the parent file.

.. _PDAL library: https://pdal.io

The same calculation can be run using the command-line tool in the following way:

.. code-block:: shell

    laserfarm retiling --input_file=point_cloud.LAZ - set_grid --min_x=-113107.81 --max_x=398892.19 --min_y=214783.87 --max_y=726783.87 --n_tiles_side=256 - split_and_redistribute - validate

Note that the various tasks with the corresponding arguments are chained using hyphens. If the tasks and arguments are
provided as a JSON configuration file, the pipeline can be executed in the following way:

.. code-block:: shell

    laserfarm retiling --input_file=point_cloud.LAZ - config --from_file=retiling_config.json - run

.. _DataProcessing:

Point-Cloud Data Processing
---------------------------

Once the raw data is split into tiles whose volume can be handled by the infrastructure available to the user,
point-cloud-based properties can be extracted. Laserfarm implements a wrapper to `laserchicken`_, which is the
engine employed to parse and process point-cloud data. The following example Python script processes a LAZ file that
contains the point-cloud subset corresponding to the ``(X=0, Y=0)`` tile in the chosen tiling scheme:

.. code-block:: python

    from laserfarm import DataProcessing

    pipeline = DataProcessing(input="tile.LAZ", tile_index=(0, 0))
    input_dict = {
        'load': {},
        'normalize': {'cell_size': 1},
        'generate_targets': {
            'min_x': -113107.81,
            'max_x': 398892.19,
            'min_y': 214783.87,
            'max_y': 726783.87,
            'n_tiles_side': 256,
            'tile_mesh_size' : 10.,
            'validate' : True,
        },
        'extract_features': {
            'volume_type': 'cell',
            'volume_size': 10.,
            'feature_names': ['point_density']
        },
        'export_targets': {}
    }
    pipeline.config(input_dict)
    pipeline.run()

.. _laserchicken: https://github.com/eEcoLiDAR/laserchicken

Also here a dictionary is employed to configure the pipeline (a JSON file could be used exactly as in :ref:`Retiling`).
The command-line tool can also be used to run the data processing pipeline (the ``data_processing`` command is issued
here):

.. code-block:: shell

    laserfarm data_processing --input=tile.LAZ --tile_index=[0,0] - load - generate_targets --min_x=-113107.81 --max_x=398892.19 --min_y=214783.87 --max_y=726783.87 --n_tiles_side=256 --tile_mesh_size=10. --validate - extract_features --feature_names=[point_density] - export_targets

or, if the configuration dictionary is serialized in the ``data_processing.json`` file:

.. code-block:: shell

    laserfarm data_processing --input=tile.LAZ --tile_index=[0,0] - config --from_file=data_processing.json - run

The full (ordered) list of tasks that can be executed within the data processing pipeline can be inspected from the
command line:

.. code-block:: shell

    laserfarm data_processing pipeline

The example pipeline above entails five steps. First, the point-cloud data is loaded into memory. Note that the input
path provided can point to either a file or a directory, in which case all files in a point-cloud format that is known
to ``laserchicken`` are considered. In order to reduce the memory requirements, one can load only the attributes that are
necessary for further data processing from the input LAZ file(s). These attributes can be provided using the optional
argument ``attributes`` of the ``DataProcessing``'s ``load`` method:

.. code-block:: python

    input_dict = {
        ...
        'load': {'attributes': ['intensity', 'gps_time']}
        ...
    }

If no attribute other than the (X, Y, Z) coordinates of the points is required, one can assign ``attributes`` with an
empty list.

The second step of the pipeline consists in the point-cloud heights' normalization, which is required for the extraction
of some of the features (see the ``laserchicken`` `manual`_. Square cells are employed for this purpose, and the length
of the cell sides (in meters) is set with the ``cell_size`` argument.

In order to extract statistical properties from the data, the point cloud must be subdivided into partitions that
represent the ensembles over which the properties are calculated. Such partitions (the 'neighborhoods') can be defined
using contiguous square cells, and the properties computed over each neighborhood assigned to the cells' centroids
(see also the ``laserchicken`` `manual`_). For a given tile the full set of centroids, i.e. the target points, is
generated by the ``generate_targets`` method, which requires information about the tiling scheme and the desired mesh
size of the target grid (``tile_mesh_size``, in meters). Note that ``tile_mesh_size`` ultimately sets the desired
resolution of the raster maps, since it corresponds to the pixel size in the final GeoTIFFs. If ``validate`` is set to
true, the points belonging to the input point cloud are checked to lie within the boundaries of the tile for which
target points are generated (recommended).

Once the target point set is generated, the desired properties of the input point cloud can be computed. The example
above will calculate a single feature, i.e. ``point_density``, but multiple features can be extracted in a single run.
``volume_type`` and ``volume_size`` define the neighborhoods employed for the extraction of properties: by assigning
them with ``cell`` and the value employed for ``tile_mesh_size``, respectively, the neighborhoods are defined as the
cells the centroids of which are the generated target points.
Statistical properties can be computed over a subset of points in each neighborhoods (for instance, to mimic data
sets with lower point densities). This is achieved by specifying the ``sample_size`` argument to the ``extract_features``
method, which defines the number of randomly-selected points considered in each cell (all points are considered for
cells that include :math:`N\leq` ``sample_size`` points).

Finally, the target points and the associated properties are written to disk. By default, the polygon (PLY) format
is employed, with one output file including all extracted features. However, single-feature files can also be exported
by setting the ``multi_band_files`` argument to false.

Additional steps that can be optionally included in the data-processing pipeline allows the user to generate
parametrized features using the extractors available in ``laserchicken`` (see the `manual`_) and to select a subset of
the input point cloud for the feature extraction. Specific information on the required arguments can be obtained from
the corresponding command line helpers:

.. code-block:: shell

    laserfarm data_processing add_custom_feature --help

and:

.. code-block:: shell

    laserfarm data_processing apply_filter --help

.. _manual: https://laserchicken.readthedocs.io/en/latest

.. NOTE::
    ``laserchicken`` computes and caches the k-d tree of the point cloud in order to efficiently querying point-cloud
    points in the filter (with polygons), normalization and feature extraction tasks. The cache can be cleared using the
    ``clear_cache`` method in the point-cloud data processing pipeline, e.g. by setting:

    .. code-block:: python

        input_dict = {
            ...
            'clear_cache: {},
            ...
        }

GeoTIFF Export
--------------

In the last step of the full processing pipeline the properties extracted from the raw input point cloud in a tile-wise
fashion are tiled back together and exported as raster maps. The following example illustrates how to generate
a single-band GeoTIFF file for the ``point_density`` feature from a set of PLY files containing the target points for
all the tiles in which an initial LAZ file has been split:

.. code-block:: python

    from laserfarm import GeotiffWriter

    pipeline = GeotiffWriter(input_dir="/path/to/PLY/files", bands='point_density')
    input_dict = {
        'parse_point_cloud': {},
        'data_split': {'xSub': 1, 'ySub': 1},
        'create_subregion_geotiffs': {'output_handle': 'geotiff'}
    }
    pipeline.config(input_dict)
    pipeline.run()

Similarly to the re-tiling and point-cloud data-processing pipelines, the ``config`` and ``run`` methods are employed
to configure and run the pipeline, respectively. The same pipeline can be run via the command line as:

.. code-block:: shell

    laserfarm geotiff_writer --input_dir=/path/to/PLY/files --bands=point_density - parse_point_cloud - data_split --xSub=1 --ySub=1 - create_subregion_geotiffs --output_handle=geotiff

As for the other pipelines, JSON files can be used to configure the pipeline as well.
This example pipeline entails the following steps. First, the list of PLY files to be parsed is constructed and a
representative file is parsed in order to obtain information on the number or target points per tile and the spacing
between target points.

.. NOTE::
    All tiles are assumed to be square and to include the same number of target points with the same target mesh size.

For data sets with large lateral extend or very large resolution (i.e. very fine target meshes), a single GeoTIFF file
could be difficult to handle with standard GIS tools. It is thus possible to partition the area covered by the tiles
into (``xSub`` :math:`\times` ``ySub``) sub-regions and to generate a GeoTIFF for each of the sub-regions. In the example above,
``xSub = ySub = 1`` sets a single GeoTIFF file to cover all tiles.

.. NOTE::
    The sub-region dimensions should be multiple of the corresponding tile dimensions.

Finally, Laserfarm generates the GeoTIFF file(s) using `GDAL`_ (``output_handle`` is employed as file-name
handle).

.. _GDAL: https://gdal.org

Point Classification
--------------------

Laserfarm allows to classify the points belonging to a point-cloud data set using (multi-)polygons defined in
a set of files in shapefile format (``.shp``). For macro-ecology applications, this can be useful, for instance, to
classify points as part of water-bodies, buildings, vegetation, etc. In this example, the target points in
the PLY file ``tile.ply`` are classified using the shapefiles provided at a given path:

.. code-block:: python

    from laserfarm import Classification

    pipeline = Classification(input_file="tile.ply")
    input_dict = {
        'locate_shp': {'shp_dir': '/path/to/dir/with/shp/files'},
        'classification': {'ground_type': 1},
        'export_point_cloud' : {}
    }
    pipeline.config(input_dict)
    pipeline.run()

To run the same pipeline using the command-line tool:

.. code-block:: shell

    laserfarm classification --input_file=tile.ply - locate_shp --shp_dir=/path/to/dir/with/shp/files - classification --ground_type=1 - export_point_cloud

As for all the other pipelines, JSON files can be used to configure the pipeline as well.
The first task in the pipeline consists in identifying which among all shapefiles provided are relevant for the given
point-set (this is determined by checking whether any of the polygons intersect the point-cloud bounding box). Then,
the points are classified: for the points falling within the polygons, the feature ``ground_type`` is updated to ``1``
(the feature is added if not already present). Finally, the point-cloud data set is written to disk.

Pipelines with Remote Data
--------------------------

LiDAR-based macro-ecology studies could easily involve several TBs of raw point-cloud data. These data volumes are
difficult to handle on standard local machines. In addition, the data should also be accessible to the infrastructure(s)
where the processing takes place (e.g. to all the nodes of a compute cluster). In order to avoid data duplication and to
limit the disk-space requirement of the processing unit(s), a remote storage infrastructure can be used to dump the raw
data and the result of the pipeline calculations. The raw-data re-tiling, point-cloud data-processing and
GeoTIFF-writing pipelines implement methods to retrieve input and drop output to storage services using the WebDAV
protocol.

The following example shows how the example in :ref:`Retiling` can be modified to retrieve ``point_cloud.LAZ`` from the
storage facility with hostname ``https://webdav.hostname.com`` (connecting to port ``8888``) using the specified
credentials to log in:

.. code-block:: python
    :emphasize-lines: 4-9,11,12,22,23

    from laserfarm import Retiler

    pipeline = Retiler(input_file="point_cloud.LAZ")
    webdav_options = {
        'webdav_hostname': 'https://webdav.hostname.com:8888',
        'webdav_login': 'username',
        'webdav_password': 'password'
    }
    pipeline.set_wdclient(webdav_options)
    input_dict = {
        'setup_local_fs': {'tmp_folder': '/path/to/local/tmp/dir'},
        'pullremote': '/remote/path/to/input',
        'set_grid': {
            'min_x': -113107.81,
            'max_x': 398892.19,
            'min_y': 214783.87,
            'max_y': 726783.87,
            'n_tiles_side': 256
        },
        'split_and_redistribute': {},
        'validate': {},
        'pushremote': '/remote/path/to/output',
        'cleanlocalfs': {}
    }
    pipeline.config(input_dict)
    pipeline.run()

Laserfarm will create two directories for input and output as sub-folders of ``tmp_folder``, download the
input file ``point_cloud.LAZ`` from the path ``/remote/path/to/input`` on the WebDAV server to the input folder,
perform the re-tiling as described in :ref:`Retiling`, upload the results from the output folder to the remote path
``/remote/path/to/output`` on the WebDAV server and delete the local input and output folders.

It is also possible to set arbitrary paths for the input and output folders:

.. code-block:: python

    input_dict = {
        ...
        'setup_local_fs': {
            'input_folder': '/path/to/local/input/folder',
            'output_folder': '/path/to/local/output/folder'
        }
        ...
    }

The point-cloud data-processing pipeline and the GeoTIFF-exporting pipeline can be configured to retrieve input files
(or directories) from a storage service with WebDAV support in the very same way.

Macro-Pipelines
---------------

For a macro-ecology study where the point-cloud data is stored in multiple LAZ files, the re-tiling of all input files,
the feature extraction for all tiles in which the raw data is split, and the generation of GeoTIFFs for all desired
features are embarrassingly parallel tasks. The following example shows how the example in :ref:`Retiling` can be
modified to perform the re-tiling of 10 point-cloud files (``point_cloud_X.LAZ``, where ``X`` ranges from 0 to 9)
exploiting the parallelization over input files:

.. code-block:: python

    from laserfarm import Retiler, MacroPipeline

    macro = MacroPipeline()
    input_dict = {
        'set_grid': {
            'min_x': -113107.81,
            'max_x': 398892.19,
            'min_y': 214783.87,
            'max_y': 726783.87,
            'n_tiles_side': 256
        },
        'split_and_redistribute': {},
        'validate': {}
    }
    filenames = ['point_cloud_{}.LAZ'.format(n) for n in range(10)]
    macro.tasks = [Retiler(input_file=f, label=f).config(input_dict) for f in filenames]
    macro.setup_cluster(mode='local', processes=True, n_workers=2, threads_per_worker=1)
    macro.run()
    macro.print_outcome(to_file='results.txt')

The parallelization is achieved using `Dask`_, which is employed to deploy the cluster and to distribute the tasks. In
the example above, the computing cluster consists of two local processes (two 'workers') spawning one thread each
(recommended for all pipelines, and required for the feature extraction tasks that involve ``laserchicken``). Each of
the workers takes care of the execution of one task at a time until all tasks are completed.

.. NOTE::
    When performing macro-pipeline calculations including ``DataProcessing`` pipelines (see :ref:`DataProcessing`), it
    is important to include the ``clear_cache`` task in the input to avoid the cache to fill up the memory of the
    workers.

In order to distribute tasks to a cluster deployed over compute nodes using SSH, the script above can be modified in the
following way:

.. code-block:: python

    ...
    macro.setup_cluster(mode='ssh',
                        hosts=['172.17.0.1', '172.17.0.1', '172.17.0.2'],
                        connect_options={'known_hosts': None,
                                         'username': 'username',
                                         'client_keys': '.ssh/id_rsa'}
                        worker_options={'nthreads': 1, 'nprocs': 2}
                        scheduler_options={'dashboard_address': '8787'})
    ...

The first address or hostname in the host list is employed for the scheduler, all the other addresses/hostnames are
used for the workers. The ``nprocs`` and ``nthreads`` arguments set the number of workers running on each host and the
number of threads spawned by each worker, respectively. For further information we refer to the `Dask documentation`_.

Any other deployed Dask cluster can be used to distribute tasks within ``MacroPipeline`` if passed as an argument to the
``setup_cluster`` method, for instance:

.. code-block:: python

    from dask_jobqueue import SLURMCluster
    ...
    cluster = SLURMCluster(...)
    macro.setup_cluster(cluster=cluster)
    macro.run()

.. _Dask: https://dask.org
.. _Dask documentation: https://docs.dask.org/en/latest/setup/ssh.html

.. NOTE::
    No command line support is provided in Laserfarm for macro-pipeline calculations.

.. _Examples:

Examples
--------

The GitHub `repository`_ of Laserfarm includes a tutorial structured as a Jupyter notebook
(``tutorial.ipynb``). The notebook illustrates how to use Laserfarm to process a subset of the
*Actueel Hoogtebestand Nederland* (`AHN3`_) data set, from the retrieval of an example point-cloud data file in LAZ
format to the export of the extracted features to a GeoTIFF file.

.. _repository: https://github.com/eEcoLiDAR/Laserfarm
.. _AHN3: https://www.pdok.nl/introductie/-/article/actueel-hoogtebestand-nederland-ahn3-

A second notebook (``workflow.ipynb``) shows the workflow employed to process the full AHN3 data set. The
notebook illustrates how the re-tiling, point-cloud data-processing and GeoTIFF-exporting tasks have been configured
and distributed over the nodes of a compute cluster.

Finally, Python scripts and pipeline configuration files that have been used to test the various pipelines either on
local machines or on a virtual `docker-container-based cluster`_ can be found `here`_.

.. _docker-container-based cluster: https://github.com/eEcoLiDAR/dockerTestCluster
.. _here: https://github.com/eEcoLiDAR/Laserfarm/tree/master/examples

Current Limitations
-------------------

This package has been tested on data provided in a metric-based 2D-projected Cartesian coordinate system. While some of
the tools of Laserfarm could be applied to data in an ellipsoidal latitude/longitude coordinate system as well, this has
not been tested and it is generally expected to fail.
