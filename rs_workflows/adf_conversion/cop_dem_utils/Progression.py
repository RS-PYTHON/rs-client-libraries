# Vendored from the ESA EOPF Copernicus project (CPM ADF auxiliary-data-file):
# https://gitlab.eopf.copernicus.eu/cpm/adf-auxiliary-data-file/-/tree/main/scripts/cop_dem_utils

class Progression:
    """Class to track the progression"""

    def __init__(self, step_name, total):
        self.step_name = step_name
        self.conteur_final = 0
        self.total = total
        self.display_step = 0.1 * self.total
        self.display_counter = 0

    def show(self):
        """
        Diplay current progress
        """
        percent = int((self.conteur_final / self.total) * 100)
        print(f"...{percent}%", end="", flush=True)

    def one_more(self):
        if self.conteur_final == 0:
            print(self.step_name)
            print("0%", end="", flush=True)
        self.conteur_final += 1
        self.display_counter += 1
        if self.display_counter >= self.display_step:
            self.show()
            self.display_counter = 0
        if self.conteur_final == self.total:
            print(".")
