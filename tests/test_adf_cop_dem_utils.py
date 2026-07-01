# Copyright 2023-2026 Airbus, CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the helper classes in rs_workflows/adf_conversion/cop_dem_utils.

These modules are not part of the importable ``rs_workflows`` package (the
``adf_conversion`` directory has no ``__init__.py`` and its ``cop_dem_utils``
package ``__init__`` eagerly imports heavy optional dependencies such as
``skimage`` and ``dask``).  We therefore load the individual source files
directly from disk so the lightweight classes can be tested in isolation.
"""

import importlib.util
from pathlib import Path

import matplotlib
import pytest

matplotlib.use("Agg")  # headless backend, no display required

COP_DEM_DIR = Path(__file__).resolve().parents[1] / "rs_workflows" / "adf_conversion" / "cop_dem_utils"


def _load_module(file_name, module_name):
    """Load a standalone source file as a module, bypassing the package __init__."""
    spec = importlib.util.spec_from_file_location(module_name, COP_DEM_DIR / file_name)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


coordinates_mod = _load_module("Coordinates.py", "adf_coordinates")
progression_mod = _load_module("Progression.py", "adf_progression")
grid_mod = _load_module("Grid.py", "adf_grid")

Coordinates = coordinates_mod.Coordinates
Progression = progression_mod.Progression
Grid = grid_mod.Grid


# --------------------------------------------------------------------------- #
# Coordinates
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "lat, lon, expected_y, expected_x",
    [
        ("N00", "E000", 90, 180),
        ("S90", "W180", 0, 0),
        ("S45", "W090", 45, 90),
        ("N45", "E090", 135, 270),
    ],
)
def test_coordinates_geo_to_absolute(lat, lon, expected_y, expected_x):
    """Geographic labels are converted to absolute grid indices."""
    coord = Coordinates(lat=lat, lon=lon)
    assert coord.lat == lat
    assert coord.lon == lon
    assert coord.y == expected_y
    assert coord.x == expected_x


@pytest.mark.parametrize(
    "y, x, expected_lat, expected_lon",
    [
        (90, 180, "N00", "E000"),
        (0, 0, "S90", "W180"),
        (45, 90, "S45", "W090"),
        (135, 270, "N45", "E090"),
    ],
)
def test_coordinates_absolute_to_geo(y, x, expected_lat, expected_lon):
    """Absolute grid indices are converted back to geographic labels."""
    coord = Coordinates(y=y, x=x)
    assert coord.y == y
    assert coord.x == x
    assert coord.lat == expected_lat
    assert coord.lon == expected_lon


def test_coordinates_round_trip():
    """A geographic label survives a conversion round-trip."""
    start = Coordinates(lat="S45", lon="W090")
    back = Coordinates(y=start.y, x=start.x)
    assert (back.lat, back.lon) == ("S45", "W090")


def test_coordinates_identification():
    """identification() concatenates the geographic and absolute coordinates."""
    coord = Coordinates(y=90, x=180)
    assert coord.identification() == "N00_90_E000_180"


# --------------------------------------------------------------------------- #
# Progression
# --------------------------------------------------------------------------- #
def test_progression_counts_up_to_total():
    """one_more() increments the counter exactly up to the total."""
    progression = Progression("my step", 4)
    assert progression.total == 4
    assert progression.display_step == pytest.approx(0.4)
    for _ in range(4):
        progression.one_more()
    assert progression.conteur_final == 4


def test_progression_prints_header_and_end(capsys):
    """The step name is printed on the first call and a dot when complete."""
    progression = Progression("converting tiles", 3)
    for _ in range(3):
        progression.one_more()
    captured = capsys.readouterr().out
    assert "converting tiles" in captured
    assert "0%" in captured
    assert captured.rstrip().endswith(".")


def test_progression_show_reports_percentage(capsys):
    """show() prints the integer percentage of progress."""
    progression = Progression("step", 10)
    progression.conteur_final = 5
    progression.show()
    assert "...50%" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# Grid
# --------------------------------------------------------------------------- #
def test_grid_initialises_zeroed_array():
    """A new grid holds a zeroed array of the requested shape."""
    grid = Grid(size_y=2, size_x=3)
    assert grid.size == (2, 3)
    assert grid.array.shape == (2, 3)
    assert not grid.array.any()  # all zeros


def test_grid_change_value_and_get_array():
    """change_value() writes at the coordinate indices and get_array() returns it."""
    grid = Grid(size_y=4, size_x=4)

    class _Coord:
        y = 1
        x = 2

    grid.change_value(_Coord(), 7.0)
    assert grid.array[1, 2] == 7.0
    assert grid.get_array() is grid.array


def test_grid_save_grid_as_img(monkeypatch, tmp_path):
    """save_grid_as_img() renders the array and saves it at the expected path."""
    calls: dict = {}
    monkeypatch.setattr(grid_mod.plt, "imshow", lambda *a, **k: calls.setdefault("imshow", True))
    monkeypatch.setattr(
        grid_mod.plt,
        "savefig",
        lambda path, **kwargs: calls.update(path=path, kwargs=kwargs),
    )

    grid = Grid(size_y=2, size_x=2)
    grid.save_grid_as_img(str(tmp_path), name="report.png")

    assert calls["imshow"] is True
    assert calls["path"] == str(tmp_path / "report.png")
    assert calls["kwargs"]["dpi"] == 300
