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

import pytest
from rs_workflows.payload_builder import build_units_list

SCENARIOS: dict[str, dict] = {
    "8": {
        "json_path": "TaskTable_S1_ARD_generated_by_rs_python_v1.json",
        "kwargs": {"pipeline": "s1_ard_full", "processing_mode": ["nrt", "reprocessing"]},
    },
    "9": {
        "json_path": "TaskTable_S1_ARD_generated_by_rs_python_v1.json",
        "kwargs": {"unit": "calibration", "processing_mode": ["nrt"]},
    },
}

@pytest.mark.parametrize("case_id,cfg", SCENARIOS.items())
def test_build_units_list_returns_dict(case_id, cfg):# pylint: disable=unused-argument
    """Test build_units_list function"""
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
                    }
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
                    }
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
                    }
                ],
                "input_adfs": [
                    {"name": "CONFIG", "mandatory": False, "type": "filename"},
                    {"name": "DEM", "mandatory": False, "type": "folder"},
                ],
                "output_products": [
                    {"name": "reference_dem", "origin": "pipeline_internal", "mandatory": True, "type": "folder"}
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
                    {"name": "simulation_ref", "origin": "pipeline_internal", "mandatory": True, "type": "folder"}
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
                    }
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
                    }
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
                    }
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
                    }
                ],
            },
        ]
    }

    assert out == expected
