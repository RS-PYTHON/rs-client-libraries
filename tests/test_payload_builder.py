# Copyright 2025 CS Group
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

"""Test the payload_builder module"""

import json
from pathlib import Path
from typing import Any

import pytest

from rs_workflows.payload_builder import TaskTableError, build_units_list

SCENARIOS: dict[str, dict] = {
    # S3 L0
    "1": {
        "json_path": "TaskTable_S3_L0_generated_by_rs_python_v1.json",
        "kwargs": {"pipeline": "s3_l0_full", "processing_mode": None},
    },
    "2": {
        "json_path": "TaskTable_S3_L0_generated_by_rs_python_v1.json",
        "kwargs": {"pipeline": "s3_l0_full", "processing_mode": ["nrt"]},
    },
    "3": {
        "json_path": "TaskTable_S3_L0_generated_by_rs_python_v1.json",
        "kwargs": {"unit": "single_unit", "processing_mode": ["nrt"]},
    },
    "4": {
        "json_path": "TaskTable_S3_L0_generated_by_rs_python_v1.json",
        "kwargs": {"unit": "single_unit", "processing_mode": ["reprocessing"]},
    },
    # S1 L0
    "5": {
        "json_path": "TaskTable_S1_L0_generated_by_rs_python_v1.json",
        "kwargs": {"pipeline": "s1_l0_full", "processing_mode": None},
    },
    "6": {
        "json_path": "TaskTable_S1_L0_generated_by_rs_python_v1.json",
        "kwargs": {"pipeline": "s1_l0_full", "processing_mode": ["reprocessing"]},
    },
    "7": {
        "json_path": "TaskTable_S1_L0_generated_by_rs_python_v1.json",
        "kwargs": {"unit": "single_unit", "processing_mode": ["nrt"]},
    },
    # S1 ARD
    "8": {
        "json_path": "TaskTable_S1_ARD_generated_by_rs_python_v1.json",
        "kwargs": {"pipeline": "s1_ard_full", "processing_mode": ["nrt", "reprocessing"]},
    },
    "9": {
        "json_path": "TaskTable_S1_ARD_generated_by_rs_python_v1.json",
        "kwargs": {"unit": "calibration", "processing_mode": ["nrt"]},
    },
    "10": {
        "json_path": "TaskTable_S1_ARD_generated_by_rs_python_v1.json",
        "kwargs": {"unit": "reference_dem", "processing_mode": ["nrt"]},
    },
    "11": {
        "json_path": "TaskTable_S1_ARD_generated_by_rs_python_v1.json",
        "kwargs": {"unit": "reference_geometry", "processing_mode": ["nrt"]},
    },
    "12": {
        "json_path": "TaskTable_S1_ARD_generated_by_rs_python_v1.json",
        "kwargs": {"unit": "coregistration", "processing_mode": ["nrt"]},
    },
    "13": {
        "json_path": "TaskTable_S1_ARD_generated_by_rs_python_v1.json",
        "kwargs": {"unit": "geocoding", "processing_mode": ["nrt"]},
    },
    "14": {
        "json_path": "TaskTable_S1_ARD_generated_by_rs_python_v1.json",
        "kwargs": {"unit": "mosaicking", "processing_mode": ["nrt"]},
    },
}


@pytest.mark.parametrize("case_id,cfg", SCENARIOS.items())
def test_build_units_list_returns_dict(case_id, cfg):  # pylint: disable=unused-argument
    """Verify the result is a dict and that 'units' is a list whose items include the keys
    'name', 'module', 'input_products', 'input_adfs', and 'output_products'."""
    tt_path = Path(__file__).parent / "resources" / cfg["json_path"]
    with tt_path.open("r", encoding="utf-8") as f:
        tt = json.load(f)

    out = build_units_list(
        tasktable=tt,
        pipeline=cfg["kwargs"].get("pipeline"),
        unit=cfg["kwargs"].get("unit"),
        processing_mode=cfg["kwargs"].get("processing_mode"),
    )
    assert isinstance(out, dict)

    assert "units" in out, "Missing key 'units'."
    assert isinstance(out["units"], list)

    for i, unit in enumerate(out["units"]):
        assert isinstance(unit, dict), f"units[{i}] must be a dict."
        for key in ("name", "module", "input_products", "input_adfs", "output_products"):
            assert key in unit, f"Missing key '{key}' in units[{i}]."


def _valid_tasktable():
    return {
        "pipelines": [{"name": "p1", "steps": [{"order": 1, "unit_name": "u1"}]}],
        "units": [
            {
                "name": "u1",
                "module": "mod.u1",
                "input_products": [],
                "input_adfs": [],
                "output_products": [],
            },
        ],
        "io": [],
    }


def test_build_units_list_rejects_both_pipeline_and_unit():
    """Test that build_units_list raises TaskTableError when both 'pipeline' and 'unit' are provided."""
    tt = _valid_tasktable()
    with pytest.raises(TaskTableError, match='Provide either "pipeline" or "unit", not both\\.'):
        build_units_list(tt, pipeline="p1", unit="u1")


def test_build_units_list_requires_one_of_pipeline_or_unit():
    """Test that build_units_list raises TaskTableError when neither 'pipeline' nor 'unit' is provided."""
    tt = _valid_tasktable()
    with pytest.raises(TaskTableError, match='One of "pipeline" or "unit" must be provided\\.'):
        build_units_list(tt, pipeline=None, unit=None)


def test_build_units_list_invalid_tasktable_root_type():
    """Test that providing a non-dict task table raises TaskTableError with the expected message."""
    with pytest.raises(TaskTableError, match=r"Task table root must be a JSON object \(dict\)\."):
        build_units_list("not a dict", pipeline="p1")  # type: ignore[arg-type]


def test_build_units_list_missing_or_invalid_pipelines_list():
    """Test that a missing or non-list 'pipelines' field in the task table raises
    TaskTableError with the expected message."""
    tt: dict[str, Any] = {"units": [], "io": []}
    with pytest.raises(TaskTableError, match='Missing or invalid "pipelines" list in task table\\.'):
        build_units_list(tt, pipeline="p1")


def test_build_units_list_missing_or_invalid_units_list():
    """Test that a missing or non-list 'units' field in the task table raises
    TaskTableError with the expected message."""
    tt: dict[str, Any] = {"pipelines": [], "io": []}
    with pytest.raises(TaskTableError, match='Missing or invalid "units" list in task table\\.'):
        build_units_list(tt, pipeline="p1")


def test_build_units_list_missing_or_invalid_io_list():
    """Test that a missing or non-list 'io' field in the task table raises TaskTableError with the expected message."""
    tt = {
        "pipelines": [{"name": "p1", "steps": [{"order": 1, "unit_name": "u1"}]}],
        "units": [{"name": "u1", "module": "m", "input_products": [], "input_adfs": [], "output_products": []}],
    }
    with pytest.raises(TaskTableError, match='Missing or invalid "io" list in task table\\.'):
        build_units_list(tt, pipeline="p1")


def test_case_8_exact_output():
    """Test build_units_list function"""
    tt_path = Path(__file__).parent / "resources" / "TaskTable_S1_ARD_generated_by_rs_python_v1.json"
    tt = json.loads(tt_path.read_text(encoding="utf-8"))

    out = build_units_list(
        tasktable=tt,
        pipeline="s1_ard_full",
        processing_mode=["nrt", "reprocessing"],
    )

    expected = {
        "units": [
            {
                "name": "calibration",
                "module": "s1_l12_rp.computing.ard_processing_units",
                "input_products": [
                    {
                        "name": "cal_input",
                        "origin": "pipeline_input",
                        "mandatory": False,
                        "type": "folder",
                        "store_type": "safe",
                    },
                ],
                "input_adfs": [
                    {"name": "CONFIG", "mandatory": False, "type": "filename"},
                    {"name": "ETAD", "mandatory": False, "type": "folder"},
                ],
                "output_products": [
                    {
                        "name": "cal_slcs",
                        "origin": "pipeline_internal",
                        "mandatory": True,
                        "type": "folder",
                        "store_type": "safe",
                        "opening_mode": "CREATE_OVERWRITE",
                    },
                ],
            },
            {
                "name": "reference_dem",
                "module": "s1_l12_rp.computing.ard_processing_units",
                "input_products": [
                    {
                        "name": "cal_slcs",
                        "origin": "calibration.cal_slcs",
                        "mandatory": False,
                        "type": "folder",
                        "store_type": "safe",
                        "opening_mode": "CREATE_OVERWRITE",
                    },
                ],
                "input_adfs": [
                    {"name": "CONFIG", "mandatory": False, "type": "filename"},
                    {"name": "DEM", "mandatory": False, "type": "folder"},
                ],
                "output_products": [
                    {"name": "reference_dem", "origin": "pipeline_internal", "mandatory": True, "type": "folder"},
                ],
            },
            {
                "name": "reference_geometry",
                "module": "s1_l12_rp.computing.ard_processing_units",
                "input_products": [
                    {
                        "name": "cal_slcs",
                        "origin": "calibration.cal_slcs",
                        "mandatory": False,
                        "type": "folder",
                        "store_type": "safe",
                        "opening_mode": "CREATE_OVERWRITE",
                    },
                    {
                        "name": "reference_dem",
                        "origin": "reference_dem.reference_dem",
                        "mandatory": False,
                        "type": "folder",
                    },
                ],
                "input_adfs": [{"name": "CONFIG", "mandatory": False, "type": "filename"}],
                "output_products": [
                    {"name": "simulation_ref", "origin": "pipeline_internal", "mandatory": True, "type": "folder"},
                ],
            },
            {
                "name": "coregistration",
                "module": "s1_l12_rp.computing.ard_processing_units",
                "input_products": [
                    {
                        "name": "cal_slcs",
                        "origin": "calibration.cal_slcs",
                        "mandatory": False,
                        "type": "folder",
                        "store_type": "safe",
                        "opening_mode": "CREATE_OVERWRITE",
                    },
                    {
                        "name": "reference_dem",
                        "origin": "reference_dem.reference_dem",
                        "mandatory": False,
                        "type": "folder",
                    },
                    {
                        "name": "simulation_ref",
                        "origin": "reference_geometry.simulation_ref",
                        "mandatory": False,
                        "type": "folder",
                    },
                ],
                "input_adfs": [{"name": "CONFIG", "mandatory": False, "type": "filename"}],
                "output_products": [
                    {
                        "name": "cslcs",
                        "origin": "pipeline_internal",
                        "mandatory": True,
                        "type": "filename",
                        "store_type": "zarr",
                        "store_params": {"consolidate": True},
                    },
                ],
            },
            {
                "name": "geocoding",
                "module": "s1_l12_rp.computing.ard_processing_units",
                "input_products": [
                    {
                        "name": "cslcs",
                        "origin": "coregistration.cslcs",
                        "mandatory": False,
                        "type": "filename",
                        "store_type": "zarr",
                        "store_params": {"consolidate": True},
                    },
                    {
                        "name": "simulation_ref",
                        "origin": "reference_geometry.simulation_ref",
                        "mandatory": False,
                        "type": "folder",
                    },
                ],
                "input_adfs": [{"name": "CONFIG", "mandatory": False, "type": "filename"}],
                "output_products": [
                    {
                        "name": "gslcs",
                        "origin": "pipeline_internal",
                        "mandatory": True,
                        "type": "filename",
                        "store_type": "zarr",
                        "store_params": {"consolidate": True},
                    },
                ],
            },
            {
                "name": "mosaicking",
                "module": "s1_l12_rp.computing.ard_processing_units",
                "input_products": [
                    {
                        "name": "gslcs",
                        "origin": "geocoding.gslcs",
                        "mandatory": False,
                        "type": "filename",
                        "store_type": "zarr",
                        "store_params": {"consolidate": True},
                    },
                ],
                "input_adfs": [{"name": "S2_TILES", "mandatory": False, "type": "filename"}],
                "output_products": [
                    {
                        "name": "nrb",
                        "origin": "pipeline_output",
                        "mandatory": True,
                        "type": "filename",
                        "store_type": "zarr",
                        "store_params": {"consolidate": True},
                    },
                ],
            },
        ],
    }

    assert out == expected
