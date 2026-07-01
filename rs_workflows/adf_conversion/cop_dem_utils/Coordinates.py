# Vendored from the ESA EOPF Copernicus project (CPM ADF auxiliary-data-file):
# https://gitlab.eopf.copernicus.eu/cpm/adf-auxiliary-data-file/-/tree/main/scripts/cop_dem_utils

class Coordinates:
    def __init__(self, lat=None, lon=None, y=None, x=None):
        if lat is not None and lon is not None:
            self.lat = lat
            self.lon = lon
            self.y, self.x = self.convert_to_absolute(self.lat, self.lon)
        elif x is not None and y is not None:
            self.y = y
            self.x = x
            self.lat, self.lon = self.convert_to_geo(y=self.y, x=self.x)

    def convert_to_geo(self, y, x):
        lat = f"S{str(90-y).zfill(2)}" if y < 90 else f"N{str(y-90).zfill(2)}"
        lon = f"W{str(180-x).zfill(3)}" if x < 180 else f"E{str(x-180).zfill(3)}"
        return lat, lon

    def convert_to_absolute(self, lat, lon):
        y = 90 - int(lat[1:3]) if lat[0:1] == "S" else 90 + int(lat[1:3])
        x = 180 - int(lon[1:4]) if lon[0:1] == "W" else 180 + int(lon[1:4])
        return y, x

    def identification(self):
        return f"{self.lat}_{self.y}_{self.lon}_{self.x}"
