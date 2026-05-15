import os

import cop_dem_utils as cdu
import dask
from skimage.io import imread


class Tile:
    def __init__(self, s3_config, s3_path, working_directory):
        # S3 path to the tar file
        self.s3_path = s3_path
        self.s3_config = s3_config
        self.working_directory = working_directory
        self.tile_name = f"{os.path.basename(self.s3_path)}.tif"
        self.s3_tile_path = os.path.join(self.s3_path, self.tile_name)
        self.local_tile_path = os.path.join(self.working_directory, self.tile_name)
        # Coordinate of the tile, Coordinate object
        self.coordinates = self.get_coordinates(tile_name=self.tile_name)
        # Tuple of int, ex : (1201, 1201)
        self.size = (0, 0)

    def download_file(self, s3_path, local_path):
        """Method to download a tile on the s3."""
        return self.s3_config.download(s3_path, local_path)

    def get_coordinates(self, tile_name):
        tmp = tile_name.split(sep="_")
        return cdu.Coordinates(lat=tmp[4], lon=tmp[6])

    @dask.delayed
    def get_tile(self, progression):
        self.download_file(s3_path=self.s3_tile_path, local_path=self.local_tile_path)
        array = imread(self.local_tile_path)
        os.remove(self.local_tile_path)
        progression.one_more()
        return array
