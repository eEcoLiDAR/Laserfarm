import logging
import pathlib
import numpy as np
import shapefile
import shapely
import laserfarm
import laserchicken
from shapely.geometry import shape
from laserfarm.pipeline_remote_data import PipelineRemoteData
from laserchicken.io.load import load
from laserchicken.io.export import export
from laserchicken import filter

logger = logging.getLogger(__name__)


class Classification(PipelineRemoteData):
    """ Classify points using polygons provided as shapefiles. """

    def __init__(self, input_file=None, label=None):
        self.pipeline = ('locate_shp',
                         'classification',
                         'export_point_cloud')
        self.input_shp = []
        self.point_cloud = None
        if input_file is not None:
            self.input_path = input_file
        if label is not None:
            self.label = label

    def locate_shp(self, shp_dir):
        """
        Locate the corresponding ESRI shape file of the point cloud
        
        :param shp_dir: directory which contains all candidate shp file for
        classification
        """

        laserfarm.utils.check_file_exists(self.input_path,
                                          should_exist=True)
        pc = load(self.input_path.as_posix())

        shp_path = self.input_folder / shp_dir

        laserfarm.utils.check_dir_exists(shp_path, should_exist=True)

        # Get boundary of the point cloud
        self.point_cloud = pc
        x = pc[laserchicken.keys.point]['x']['data']
        y = pc[laserchicken.keys.point]['y']['data']
        point_box = shapely.geometry.box(np.min(x), np.min(y),
                                         np.max(x), np.max(y))

        for shp in sorted([f.absolute() for f in shp_path.iterdir()
                           if f.suffix == '.shp']):
            sf = shapefile.Reader(shp.as_posix())
            mbr = shapely.geometry.box(*sf.bbox)

            if point_box.intersects(mbr):
                self.input_shp.append(shp)

        return self

    def classification(self, ground_type):
        """
        Classify the pointset according to the given shape file.
        A new feature "ground_type" will be added to the point cloud.
        The value of the column identify the ground type.
        
        :param ground_type: identifier of the groud type. 0 is not identified.
        """

        # Get the mask of points which fall in the shape file(s)
        pc_mask = np.zeros(len(self.point_cloud['vertex']['x']['data']),
                           dtype=bool)
        for shp in self.input_shp:
            this_mask = filter.select_polygon(self.point_cloud,
                                              shp.as_posix(),
                                              read_from_file=True,
                                              return_mask=True)
            pc_mask = np.logical_or(pc_mask, this_mask)

        # Add the ground type feature 
        laserchicken.utils.update_feature(self.point_cloud,
                                          feature_name='ground_type',
                                          value=ground_type,
                                          array_mask=pc_mask)
        # Clear the cached KDTree
        laserchicken.kd_tree.initialize_cache()
        return self

    def export_point_cloud(self, filename='', overwrite=False):
        """
        Export the classified point cloud

        :param filename: filename where to write point-cloud data
        :param overwrite: if file exists, overwrite
        """
        if pathlib.Path(filename).parent.name:
            raise IOError('filename should not include path!')
        if not filename:
            filename = '_classification'.join([self.input_path.stem,
                                               self.input_path.suffix])
        export_path = (self.output_folder / filename).as_posix()

        export(self.point_cloud, export_path, overwrite=overwrite)

        return self
