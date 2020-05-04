import logging
import pathlib
import numpy as np
import shapefile
import shapely
import lc_macro_pipeline
import laserchicken
from shapely.geometry import shape
from lc_macro_pipeline.pipeline_remote_data import PipelineRemoteData
from laserchicken.io.load import load
from laserchicken.io.export import export
from laserchicken import filter


logger = logging.getLogger(__name__)

class Classification(PipelineRemoteData):
    """ Classify point cloud w.r.t. the kadaster data """

    def __init__(self):
        self.pipeline = ('locate_shp',
                         'classification',
                         'export_point_cloud')
        self.input_shp = []
        self.point_cloud = None

    def locate_shp(self, point_cloud, shp_dir):
        """
        Locate the corresponding ESRI shape file of the point cloud
        
        :param point_cloud: target point cloud for classification. 
                            Path to the point cloud file (relative to self.output_folder root)
                            or point cloud instance 
        :param shp_dir: directory which contains all candidate shp file for classification
        """
        if isinstance(point_cloud, str):
            pc_path=self.input_folder/point_cloud
            lc_macro_pipeline.utils.check_path_exists(pc_path, should_exist=True)
            pc = load(pc_path.as_posix())
        else:
            pc = point_cloud
        
        shp_path=self.input_folder/shp_dir
        
        lc_macro_pipeline.utils.check_path_exists(shp_path, should_exist=True)

        # Get boundary of the point cloud
        self.point_cloud = pc
        x = pc[laserchicken.keys.point]['x']['data']
        y = pc[laserchicken.keys.point]['y']['data']
        point_box = shapely.geometry.box(np.min(x), np.min(y), np.max(x), np.max(y))

        for shp in sorted([f.absolute() for f in shp_path.iterdir() if f.suffix=='.shp']):
            sf = shapefile.Reader(shp.as_posix())
            mbr = shape(sf.shapeRecords()[0].shape.__geo_interface__).envelope
            
            if point_box.intersects(mbr):
                self.input_shp.append(shp)
        
        return self


    def classification(self, ground_type):
        """
        Classify the pointset according to the given shape file.
        A new feature "ground_type" will be added to the point cloud. The value of the column identify the ground type.
        
        :param ground_type: identifier of the groud type. 0 is not identified.
        """

        # Get the mask of points which fall in the shape file(s)
        pc_mask = np.zeros(len(self.point_cloud['vertex']['x']['data']), dtype=bool)
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

        return self


    def export_point_cloud(self, filename='', overwrite=False):
        """
        Export the classified point cloud
        :param filename: filename where to write point-cloud data
                         (relative to self.output_folder root)
        """
        if pathlib.Path(filename).parent.name:
            raise IOError('filename should not include path!')
        export_path = (self.output_folder/filename).as_posix()

        export(self.point_cloud, export_path, overwrite=overwrite)

        return self
   
