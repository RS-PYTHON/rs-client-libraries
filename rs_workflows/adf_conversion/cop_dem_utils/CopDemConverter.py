# Vendored from the ESA EOPF Copernicus project (CPM ADF auxiliary-data-file):
# https://gitlab.eopf.copernicus.eu/cpm/adf-auxiliary-data-file/-/tree/main/scripts/cop_dem_utils

import os
import shutil

import cop_dem_utils as cdu
import dask
import numpy as np


class CopDemConverter:
    @staticmethod
    def reset_scratch(scratch_path):
        """Function to clean the scratch repertory."""
        if os.path.isdir(scratch_path):
            shutil.rmtree(scratch_path)
        os.mkdir(scratch_path)

    def __init__(self, working_directory, s3_config, s3_bucket, grid_size_y=180, grid_size_x=360):
        self.working_directory = working_directory
        self.s3_config = s3_config
        self.s3_bucket = s3_bucket
        self.ref_grid = cdu.Grid(size_y=grid_size_y, size_x=grid_size_x)
        self.no_data = 0

    def gen_s3_key_list(self, s3_config, s3_bucket):
        tmp = s3_config.ls(s3_bucket)[2:-1]
        ret = []
        for elem in tmp:
            if elem.startswith("copernicus-dem-30m/Copernicus_DSM_COG_10_"):
                ret.append(elem)
            elif elem.startswith("copernicus-dem-90m/Copernicus_DSM_COG_30_"):
                ret.append(elem)
        return ret

    def gen_tiles_dict_ref(self, s3_keys):
        prg = cdu.Progression("1/5: Make tile dict", len(s3_keys))
        tiles_dict = {}
        for file in s3_keys:
            CopDemConverter.reset_scratch(scratch_path=self.working_directory)
            tile_tmp = cdu.Tile(s3_config=self.s3_config, s3_path=file, working_directory=self.working_directory)
            if tile_tmp.coordinates.identification() not in tiles_dict:
                tiles_dict[tile_tmp.coordinates.identification()] = tile_tmp
                self.ref_grid.change_value(tile_tmp.coordinates, 1)
                prg.one_more()
        return tiles_dict

    @dask.delayed
    def gen_empty_tile(self, size, fill_value, type_of=np.dtype("float32")):
        # return None
        return np.full(size, fill_value, dtype=type_of)

    def gen_lat_dict(self, ref_grid, tiles_dict, tiles_size):
        prg = cdu.Progression("2/5: Gen lat dict", 64800)
        # Init the different arrays
        arrays_dict = {}
        # Init on empty array for each dim
        empty_tile = {}
        for lat_ids in tiles_size.keys():
            arrays_dict[lat_ids] = {}
            empty_tile[lat_ids] = self.gen_empty_tile(size=tiles_size[lat_ids]["size"], fill_value=np.nan)

        for (y, x), value in np.ndenumerate(ref_grid):
            prg.one_more()
            # Add empty [] for new lat
            for array_name in arrays_dict.keys():
                if y not in arrays_dict[array_name]:
                    arrays_dict[array_name][y] = []
            # If tile
            if value == 1:
                coord = cdu.Coordinates(y=y, x=x)
                tile = tiles_dict[coord.identification()]
                tile_array = tile.get_tile(self.wprg)
                for array_name in arrays_dict.keys():
                    inter_min = tiles_size[array_name]["min"]
                    inter_max = tiles_size[array_name]["max"]
                    if inter_min <= y < inter_max:
                        arrays_dict[array_name][y].append(tile_array)
                    else:
                        arrays_dict[array_name][y].append(empty_tile[array_name])
            # If no tile
            else:
                for array_name in arrays_dict.keys():
                    arrays_dict[array_name][y].append(empty_tile[array_name])
        return arrays_dict

    def merge_lon(self, arrays_dict, tiles_size):
        nb_lat_groups = len(arrays_dict)
        prg = cdu.Progression("3/5: Merge longitude on same latitude", nb_lat_groups * 180)
        # Init the different arrays
        merged_lat_arrays = {}

        for lat_group in arrays_dict.keys():
            # initialize this lattitude group
            merged_lat_arrays[lat_group] = {}
            for lat in arrays_dict[lat_group]:
                size = tiles_size[lat_group]["size"]
                delayed_tiles_at_lat = [
                    dask.array.from_delayed(dem_array, dtype=np.dtype("float32"), shape=size)
                    for dem_array in arrays_dict[lat_group][lat]
                ]
                merged_lat_arrays[lat_group][lat] = dask.array.concatenate(delayed_tiles_at_lat, axis=1)
                prg.one_more()
        return merged_lat_arrays

    def merge_lat(self, merged_lat_arrays):
        nb_lat_groups = len(merged_lat_arrays)
        prg = cdu.Progression("4/5: Merge latitude", nb_lat_groups * 180)
        merged_dem = {}
        for array in merged_lat_arrays:
            tiles_in_lat_group = []
            for lat in list(reversed(merged_lat_arrays[array].keys())):
                tiles_in_lat_group.append(merged_lat_arrays[array][lat])
                prg.one_more()
            merged_dem[array] = dask.array.concatenate(tiles_in_lat_group, axis=0)
        return merged_dem

    def run(self, tiles_size):
        s3_keys = self.gen_s3_key_list(s3_config=self.s3_config, s3_bucket=self.s3_bucket)
        self.wprg = cdu.Progression("5/5: Writing product", len(s3_keys))
        tiles_dict = self.gen_tiles_dict_ref(s3_keys=s3_keys)
        self.ref_grid.save_grid_as_img(path=self.working_directory)
        arrays_dict = self.gen_lat_dict(self.ref_grid.get_array(), tiles_dict, tiles_size)
        arrays_dict = self.merge_lon(arrays_dict, tiles_size)
        return self.merge_lat(arrays_dict)
