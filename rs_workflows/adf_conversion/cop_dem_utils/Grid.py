# Vendored from the ESA EOPF Copernicus project (CPM ADF auxiliary-data-file):
# https://gitlab.eopf.copernicus.eu/cpm/adf-auxiliary-data-file/-/tree/main/scripts/cop_dem_utils

import os

import matplotlib.pyplot as plt
import numpy as np


class Grid:
    def __init__(self, size_y=180, size_x=360):
        self.size = (size_y, size_x)
        self.array = np.zeros([size_y, size_x])

    def save_grid_as_img(self, path, name="Grid_report.png"):
        plt.imshow(self.array, interpolation="nearest", extent=[-180, 180, 90, -90])
        plt.savefig(os.path.join(path, name), dpi=300)

    def change_value(self, coordinates, val):
        self.array[coordinates.y, coordinates.x] = val

    def get_array(self):
        return self.array
