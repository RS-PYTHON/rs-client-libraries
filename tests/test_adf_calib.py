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

"""Local AUX_CAL conversion and catalog integration tests."""

import json
import shutil
import subprocess
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest
import xarray as xr
from pystac import Asset, Item, ItemCollection

from rs_workflows import adf_flow
from rs_workflows.flow_utils import (
    AdfProcessIn,
    AdfType,
    AuxiliaryProductMapping,
    FlowEnvArgs,
)

SAFE_NAME = "S1A_AUX_CAL_V20190228T092500_G20240327T102320.SAFE"
PRODUCT_ID = "S01A_ADF_CALIB_V20190228T092500_G20240327T102320"
START = "2019-02-28T09:25:00Z"
END = "2099-12-31T23:59:59Z"

pytestmark = pytest.mark.filterwarnings(
    "ignore:Logger 'prefect\\.task_runs' attempted to send logs to the API without a flow run id\\.:UserWarning",
)


@pytest.fixture(name="calibration_input")
def fixture_calibration_input(tmp_path, monkeypatch):
    """Create a small, real calibration XML and staged product metadata."""
    monkeypatch.setattr(adf_flow, "get_logger", MagicMock())
    input_dir = tmp_path / "INPUT"
    work_dir = tmp_path / "WORK"
    output_dir = tmp_path / "OUTPUT"
    xml_path = input_dir / SAFE_NAME / "data" / "s1a-aux-cal.xml"
    xml_path.parent.mkdir(parents=True)
    work_dir.mkdir()
    output_dir.mkdir()
    params = """
      <calibrationParams>
        <swath>{swath}</swath><polarisation>{polarisation}</polarisation>
        <elevationAntennaPattern>
          <beamNominalNearRange>17.0</beamNominalNearRange>
          <beamNominalFarRange>23.0</beamNominalFarRange>
          <elevationAngleIncrement>0.5</elevationAngleIncrement>
          <values>1.0 2.0 3.0 4.0 5.0 6.0</values>
        </elevationAntennaPattern>
        <azimuthAntennaPattern>
          <azimuthAngleIncrement>1.0</azimuthAngleIncrement>
          <values>7.0 8.0 9.0</values>
        </azimuthAntennaPattern>
        <azimuthAntennaElementPattern>
          <azimuthAngleIncrement>0.0</azimuthAngleIncrement>
          <values>10.0</values>
        </azimuthAntennaElementPattern>
        <absoluteCalibrationConstant>11.0</absoluteCalibrationConstant>
        <noiseCalibrationFactor>12.0</noiseCalibrationFactor>
      </calibrationParams>
    """
    xml_path.write_text(
        "<auxiliaryCalibration><calibrationParamsList>"
        + params.format(swath="IW1", polarisation="VV")
        + params.format(swath="IW2", polarisation="VH")
        + "</calibrationParamsList></auxiliaryCalibration>",
        encoding="utf-8",
    )
    source = Item(
        id=SAFE_NAME,
        geometry=None,
        bbox=None,
        datetime=None,
        start_datetime=datetime(2019, 2, 28, 9, 25, tzinfo=timezone.utc),
        end_datetime=datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        properties={"product:type": "AUX_CAL"},
    )
    source.add_asset("data", Asset(href=f"s3://bucket/{SAFE_NAME}.zip"))
    (work_dir / "aux_cal_items.json").write_text(json.dumps([source.to_dict()]), encoding="utf-8")
    return input_dir, work_dir, output_dir, source


def run_conversion(calibration_input):
    """Execute the actual standalone adapter in the current Python environment."""
    input_dir, work_dir, output_dir, _ = calibration_input
    return adf_flow.run_adf_script.fn(adf_flow.ADF_CALIB_SCRIPT_PATH, input_dir, work_dir, output_dir)


def test_local_calibration_conversion(calibration_input):
    """Verify actual Zarr values, group structure, metadata and catalog compatibility."""
    products = run_conversion(calibration_input)
    assert [product.name for product in products] == [f"{PRODUCT_ID}.zarr"]
    with xr.open_datatree(products[0], engine="zarr", consolidated=False) as tree:
        for group in ("IW1/VV", "IW2/VH"):
            assert tree[group].attrs == {
                "absolute_calibration_constant": 11.0,
                "noise_calibration_factor": 12.0,
            }
            elevation = tree[f"{group}/elevation_antenna_pattern"]["elevation_antenna_pattern"]
            np.testing.assert_array_equal(elevation.values, [1 + 2j, 3 + 4j, 5 + 6j])
            np.testing.assert_array_equal(elevation.angle.values, [-0.5, 0, 0.5])
            assert elevation.attrs == {"beam_nominal_near_range": 17.0, "beam_nominal_far_range": 23.0}
            azimuth = tree[f"{group}/azimuth_antenna_pattern"]["azimuth_antenna_pattern"]
            np.testing.assert_array_equal(azimuth.values, [7, 8, 9])
            np.testing.assert_array_equal(azimuth.angle.values, [-1, 0, 1])
            element = tree[f"{group}/azimuth_antenna_element_pattern"]["azimuth_antenna_element_pattern"]
            np.testing.assert_array_equal(element.values, [10])
            np.testing.assert_array_equal(element.angle.values, [0])
    item = adf_flow.create_stac_item_from_zarr.fn(products[0], "ADF_CALIB")
    assert item.id == PRODUCT_ID
    assert item.properties["platform"] == "sentinel-1a"
    assert item.properties["start_datetime"] == START
    assert item.properties["end_datetime"] == END
    assert item.properties["product:type"] == "ADF_CALIB"
    assert item.assets["data"].media_type == "application/vnd+zarr"


@pytest.mark.parametrize("platform", ["B", "C", "D"])
def test_calibration_platform_and_asset_matching(calibration_input, platform):
    """Resolve SAFE archive assets even when the catalog uses a different item ID."""
    input_dir, work_dir, _, source = calibration_input
    safe_name = SAFE_NAME.replace("S1A", f"S1{platform}")
    (input_dir / SAFE_NAME).rename(input_dir / safe_name)
    source.id = "catalog-specific-id"
    source.assets["data"].href = f"s3://bucket/{safe_name}.tar.gz"
    (work_dir / "aux_cal_items.json").write_text(json.dumps([source.to_dict()]), encoding="utf-8")
    products = run_conversion(calibration_input)
    item = adf_flow.create_stac_item_from_zarr.fn(products[0], "ADF_CALIB")
    assert item.id == PRODUCT_ID.replace("S01A", f"S01{platform}")
    assert item.properties["platform"] == f"sentinel-1{platform.lower()}"


def test_calibration_batch_preserves_each_validity(calibration_input):
    """Multiple extracted products must retain their own ADGS validity dates."""
    input_dir, work_dir, _, source = calibration_input
    second_name = SAFE_NAME.replace("20190228T092500", "20200228T092500")
    shutil.copytree(input_dir / SAFE_NAME, input_dir / second_name)
    second = source.clone()
    second.id = second_name
    second.assets["data"].href = f"s3://bucket/{second_name}.zip"
    second.properties["start_datetime"] = "2020-02-28T09:25:00Z"
    source.assets["thumbnail"] = Asset(href="https://example.test/preview.png")
    second.assets["thumbnail"] = Asset(href="https://example.test/preview.png")
    (work_dir / "aux_cal_items.json").write_text(json.dumps([source.to_dict(), second.to_dict()]), encoding="utf-8")
    products = run_conversion(calibration_input)
    assert len(products) == 2
    items = [adf_flow.create_stac_item_from_zarr.fn(product, "ADF_CALIB") for product in products]
    assert {item.properties["start_datetime"] for item in items} == {START, "2020-02-28T09:25:00Z"}


@pytest.mark.parametrize(
    "failure",
    [
        "missing_xml",
        "invalid_safe",
        "missing_metadata",
        "missing_end",
        "odd_pattern",
        "ambiguous_metadata",
        "duplicate_output",
    ],
)
def test_calibration_conversion_fails_explicitly(calibration_input, failure):
    """Invalid input must not produce success-shaped results."""
    input_dir, work_dir, _, source = calibration_input
    xml_path = input_dir / SAFE_NAME / "data" / "s1a-aux-cal.xml"
    if failure == "missing_xml":
        xml_path.unlink()
    elif failure == "invalid_safe":
        (input_dir / SAFE_NAME).rename(input_dir / "invalid.SAFE")
    elif failure == "missing_metadata":
        (work_dir / "aux_cal_items.json").write_text("[]", encoding="utf-8")
    elif failure == "missing_end":
        source.properties.pop("end_datetime")
        (work_dir / "aux_cal_items.json").write_text(json.dumps([source.to_dict()]), encoding="utf-8")
    elif failure == "odd_pattern":
        xml_path.write_text(xml_path.read_text().replace("1.0 2.0 3.0 4.0 5.0 6.0", "1.0 2.0 3.0"))
    elif failure == "ambiguous_metadata":
        other = source.clone()
        other.properties["end_datetime"] = START
        (work_dir / "aux_cal_items.json").write_text(
            json.dumps([source.to_dict(), other.to_dict()]),
            encoding="utf-8",
        )
    else:
        run_conversion(calibration_input)
    with pytest.raises(subprocess.CalledProcessError) as error:
        run_conversion(calibration_input)
    messages = {
        "missing_xml": "No AUX_CAL data XML files",
        "invalid_safe": "Invalid AUX_CAL SAFE product name",
        "missing_metadata": "No staged AUX_CAL metadata",
        "missing_end": "must have start_datetime and end_datetime",
        "odd_pattern": "legacy AUX_CAL: odd elevation pattern length",
        "ambiguous_metadata": "Ambiguous staged AUX_CAL metadata",
        "duplicate_output": "Duplicate AUX_CAL output",
    }
    assert messages[failure] in error.value.stderr


@pytest.mark.asyncio
async def test_calibration_flow_stages_converts_and_publishes(
    calibration_input,
    monkeypatch,
    _mock_os_env,
):  # pylint: disable=unused-argument
    """Mock only external services, retaining real conversion and STAC creation."""
    input_dir, _, _, source = calibration_input
    staging = AsyncMock(return_value=(True, ItemCollection([source])))
    upload = AsyncMock()
    publish = AsyncMock()
    monkeypatch.setattr(adf_flow, "aux_staging_task", staging)

    async def extract(items, destination):
        assert items[0].id == SAFE_NAME
        shutil.copytree(input_dir / SAFE_NAME, destination / SAFE_NAME)

    monkeypatch.setattr(adf_flow, "download_and_extract_assets_task", AsyncMock(side_effect=extract))
    monkeypatch.setattr(adf_flow, "run_adf_script", adf_flow.run_adf_script.fn)
    monkeypatch.setattr(adf_flow, "create_stac_item_from_zarr", adf_flow.create_stac_item_from_zarr.fn)
    monkeypatch.setattr(adf_flow, "s3_upload_dir", upload)
    monkeypatch.setattr(adf_flow, "publish", publish)
    monkeypatch.setattr(adf_flow, "fetch_csv_from_endpoint", MagicMock(return_value=[]))
    monkeypatch.setattr(adf_flow, "find_s3_output_bucket", MagicMock(return_value="test-bucket"))
    flow_env = MagicMock(owner_id="test-user")
    monkeypatch.setattr(adf_flow, "FlowEnv", MagicMock(return_value=flow_env))
    adf_input = AdfProcessIn(
        env=FlowEnvArgs(owner_id="test-user"),
        adf_type=AdfType.S01__ADF_CALIB,
        start_datetime=source.common_metadata.start_datetime,
        end_datetime=source.common_metadata.end_datetime,
        auxiliary_product_to_collection_identifier=[
            AuxiliaryProductMapping(product_type="AUX_CAL", collection_name="AUX_INPUT"),
            AuxiliaryProductMapping(product_type="ADF_CALIB", collection_name="ADF_OUTPUT"),
        ],
    )
    await adf_flow.adf_conversion.fn(adf_input)
    staging.assert_awaited_once()
    assert staging.call_args.kwargs["cql2_filter"]["filter"]["args"][1]["args"][1] == "AUX_CAL"
    assert staging.call_args.kwargs["source"].value == "auxip"
    assert staging.call_args.kwargs["catalog_collection_identifier"] == "AUX_INPUT"
    upload.assert_awaited_once()
    publish.assert_awaited_once()
    mapping, metadata = publish.call_args.args[1:]
    assert mapping[0].collection_name == "ADF_OUTPUT"
    assert metadata[0].product_type == "ADF_CALIB"
    item = metadata[0].stac_item
    assert item.id == PRODUCT_ID
    assert item.properties["start_datetime"] == START
    assert item.properties["end_datetime"] == END
    assert item.assets["data"].href == f"s3://test-bucket/test-user/ADF_OUTPUT/{PRODUCT_ID}.zarr/"
