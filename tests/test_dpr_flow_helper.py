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
"""test of helper functions for dpr_flow.py"""

import datetime
import json
import typing
from pathlib import Path

import pytest

from rs_client.ogcapi import dpr_client
from rs_client.ogcapi.dpr_client import ClusterInfo, DprProcessor
from rs_workflows.dpr_flow import (
    clean_paths,
    compute_eopf_origin_datetime,
    create_stac_item,
    extract_products_and_zattrs,
    read_zattrs_sync,
    run_processor,
    s3_download_file_sync,
    s3_list,
    update_eopf_assets,
)
from rs_workflows.flow_utils import (
    DprProcessedItemMetadata,
    FlowEnvArgs,
    FlowInputProduct,
)
from rs_workflows.payload_generator import RSPY_CATALOG_BUCKET
from tests.conftest import MOCKED_BUCKET, OWNER_ID
from tests.test_utils import setup_worklow_test_env

CLUSTER_INFO = ClusterInfo("", "", "")


def test_s3_list_returns_full_s3_paths(mocker):
    """
    Verify that s3_list returns fully-qualified S3 paths for all objects
    found under a given prefix and that the S3 filter is called with the
    correctly normalized prefix.
    """
    s3_prefix = "s3://my-bucket/some/prefix"

    mock_s3_bucket = mocker.Mock()
    mock_s3_bucket.bucket_name = "my-bucket"

    mock_bucket_resource = mocker.Mock()
    mock_objects = mocker.Mock()

    mock_bucket_resource.objects = mock_objects
    mock_s3_bucket._get_bucket_resource.return_value = mock_bucket_resource  # pylint: disable=protected-access

    mock_obj_1 = mocker.Mock()
    mock_obj_1.key = "some/prefix/file1.txt"

    mock_obj_2 = mocker.Mock()
    mock_obj_2.key = "some/prefix/file2.txt"

    mock_objects.filter.return_value = [mock_obj_1, mock_obj_2]

    mocker.patch(
        "rs_workflows.dpr_flow.prefect_utils.get_s3_bucket",
        return_value=(mock_s3_bucket, "some/prefix"),
    )

    result = s3_list(s3_prefix)

    assert result == [
        "s3://my-bucket/some/prefix/file1.txt",
        "s3://my-bucket/some/prefix/file2.txt",
    ]

    mock_objects.filter.assert_called_once_with(Prefix="some/prefix/")


def test_read_zattrs_sync_downloads_and_parses_json(mocker):
    """
    Verify that read_zattrs_sync correctly downloads each .zattrs file from S3,
    reads its JSON content, and returns a list of dictionaries containing both
    the original path and the parsed data.
    """
    zattrs_path = "s3://my-bucket/product_a/.zattrs"

    fake_json_data = {
        "foo": "bar",
        "answer": 42,
    }

    def fake_s3_download(src, dest, _sync):  # pylint: disable=unused-argument
        with open(dest, "w", encoding="utf-8") as f:
            json.dump(fake_json_data, f)

    mock_download = mocker.patch(
        "rs_workflows.dpr_flow.s3_download_file_sync",
        side_effect=fake_s3_download,
    )

    result = read_zattrs_sync(zattrs_path)

    assert result == fake_json_data

    assert mock_download.call_count == 1
    mock_download.assert_any_call(zattrs_path, mocker.ANY, _sync=True)


def test_extract_products_and_zattrs():
    """
    Verify that extract_products_and_zattrs correctly filters and extracts
    product names and .zattrs file paths from a list of files.

    The function should:
    - Include files that are exactly two levels deep: base_path/product_name/.zattrs
    - Exclude files that are deeper in the directory structure
    - Exclude files that are not named .zattrs
    - Handle various base_path formats (with/without trailing slash)
    """
    base_path = "s3://my-bucket/output"

    files = [
        # Valid .zattrs files (should be included)
        "s3://my-bucket/output/product_a/.zattrs",
        "s3://my-bucket/output/product_b/.zattrs",
        "s3://my-bucket/output/S03OLCL0_/.zattrs",
        # Too deep - nested subdirectories (should be excluded)
        "s3://my-bucket/output/product_c/subdir/.zattrs",
        "s3://my-bucket/output/product_d/deep/nested/.zattrs",
        # Not .zattrs files (should be excluded)
        "s3://my-bucket/output/product_e/data.json",
        "s3://my-bucket/output/product_f/metadata.xml",
        # Too shallow - directly under base_path (should be excluded)
        "s3://my-bucket/output/.zattrs",
        # Other files in product directories (should be excluded)
        "s3://my-bucket/output/product_a/data.zarr",
        "s3://my-bucket/output/product_b/config.yaml",
    ]

    result = extract_products_and_zattrs(files, base_path)

    # Should only include the three valid .zattrs files
    assert len(result) == 3
    assert ("product_a", "s3://my-bucket/output/product_a/.zattrs") in result
    assert ("product_b", "s3://my-bucket/output/product_b/.zattrs") in result
    assert ("S03OLCL0_", "s3://my-bucket/output/S03OLCL0_/.zattrs") in result


def test_extract_products_and_zattrs_with_trailing_slash():
    """
    Verify that extract_products_and_zattrs works correctly when base_path
    has a trailing slash.
    """
    base_path = "s3://my-bucket/output/"  # Note the trailing slash

    files = [
        "s3://my-bucket/output/product_a/.zattrs",
        "s3://my-bucket/output/product_b/.zattrs",
    ]

    result = extract_products_and_zattrs(files, base_path)

    assert len(result) == 2
    assert ("product_a", "s3://my-bucket/output/product_a/.zattrs") in result
    assert ("product_b", "s3://my-bucket/output/product_b/.zattrs") in result


def test_extract_products_and_zattrs_empty_list():
    """
    Verify that extract_products_and_zattrs returns an empty list when
    given an empty file list.
    """
    result = extract_products_and_zattrs([], "s3://my-bucket/output")
    assert not result


def test_extract_products_and_zattrs_no_matches():
    """
    Verify that extract_products_and_zattrs returns an empty list when
    no files match the expected pattern.
    """
    base_path = "s3://my-bucket/output"
    files = [
        "s3://my-bucket/output/product_a/data.json",
        "s3://my-bucket/output/product_b/subdir/.zattrs",
        "s3://my-bucket/output/.zattrs",
    ]

    result = extract_products_and_zattrs(files, base_path)
    assert not result


def test_s3_download_file_sync_downloads_and_returns_path(mocker):
    """
    Verify that s3_download_file_sync calls the underlying S3 bucket's
    download_object_to_path with the correct arguments and returns the
    destination path unchanged.
    """
    s3_path = "s3://my-bucket/some/file.txt"
    to_path = Path("/tmp/file.txt")

    mock_s3_bucket = mocker.Mock()
    mock_from_path = "some/file.txt"

    mocker.patch(
        "rs_workflows.dpr_flow.prefect_utils.get_s3_bucket",
        return_value=(mock_s3_bucket, mock_from_path),
    )

    result = s3_download_file_sync(
        s3_path,
        to_path,
        _sync=True,
        extra_arg="value",
    )

    mock_s3_bucket.download_object_to_path.assert_called_once_with(
        mock_from_path,
        str(to_path),
        _sync=True,
        extra_arg="value",
    )

    assert result == to_path


def test_create_stac_items_builds_items_with_assets_and_eopf_metadata(mocker):
    """
    Verify that create_stac_item constructs STAC Items correctly from
    EOPF feature dictionaries, injects mandatory properties, and attaches
    assets corresponding to output products.

    This test mocks Item and Asset constructors and ensures:
    - Each feature results in one Item being created
    - STAC properties like stac_version are correctly set
    - Assets are built with correct href, title, media_type, and extra_fields
    - Assets are attached to the corresponding Item
    """

    eopf_feature = {
        "id": "feature_1",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [0, 0, 0, 0],
        "properties": {
            "datetime": "2024-01-01T00:00:00",
        },
    }

    mock_item_cls = mocker.patch("rs_workflows.dpr_flow.Item")
    mock_asset_cls = mocker.patch("rs_workflows.dpr_flow.Asset")

    mock_item = mocker.Mock()
    mock_item.id = "feature_1"
    mock_item.assets = {}

    mock_item_cls.return_value = mock_item

    create_stac_item(
        eopf_origin_datetime="2024-01-10T12:00:00",
        eopf_feature=eopf_feature,
        s3_data_location="s3://my-bucket/output/feature_1.zarr",
        product_name="feature_1.zarr",
        dpr_processor=DprProcessor.S1L0,
    )

    first_call_kwargs = mock_item_cls.call_args_list[0].kwargs
    assert first_call_kwargs["id"] == "feature_1.zarr"
    assert isinstance(first_call_kwargs["datetime"], datetime.datetime)
    assert first_call_kwargs["properties"]["eopf:origin_datetime"] == "2024-01-10T12:00:00"
    assert first_call_kwargs["properties"]["stac_version"] == "1.1.0"
    assert "feature_1.zarr" in first_call_kwargs["assets"]

    # Asset built correctly
    mock_asset_cls.assert_called_with(
        href="s3://my-bucket/output/feature_1.zarr",
        title="feature_1.zarr",
        media_type="application/vnd+zarr",
        roles=["data", "metadata"],
        # the following is commented out in the actual code
        # search for RSPY-280 in the dpr_flow.py file
        # extra_fields=mocker.ANY,
    )


@typing.no_type_check
def test_update_eopf_assets_happy_path(mocker, mocked_processor_output):
    """
    Verify that the update_eopf_assets Prefect task correctly orchestrates
    discovery and processing of EOPF products:

    - Lists all files under the output S3 path
    - Extracts product names and associated .zattrs metadata
    - Reads and parses the .zattrs metadata
    - Extracts EOPF product types
    - Builds STAC items from the extracted metadata
    - Returns the STAC items and the list of product types
    """
    env = mocker.Mock()
    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mocker.Mock())

    s3_path, expected_items = mocked_processor_output

    payload = mocker.Mock()
    payload.io.output_products = [
        mocker.Mock(id="dummy_prod_id", path=f"s3://{RSPY_CATALOG_BUCKET}/{s3_path}/"),
    ]

    # Call method
    items_metadata = update_eopf_assets.fn(
        env=env,
        input_products=[
            FlowInputProduct(name="input_name", item_id="dummy_id", collection_name="dummy_collection"),
        ],
        payload=payload,
        dpr_processor=DprProcessor.S1L0,
    )

    # Check results
    assert [asset.product_type for asset in items_metadata] == [
        expected_item["properties"]["product:type"] for expected_item in expected_items.values()
    ]
    assert {asset.stac_item.id: asset.stac_item.to_dict() for asset in items_metadata} == expected_items
    for asset in items_metadata:
        assert asset.output_product_id == "dummy_prod_id"


def test_update_eopf_assets_raises_on_missing_zattrs(mocker):
    """
    Verify that update_eopf_assets raises RuntimeError when a .zattrs file
    cannot be read (e.g., read_zattrs_sync returns None).
    """
    env = mocker.Mock()
    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mocker.Mock())

    # Mock extract_products_and_zattrs to return one product
    mocker.patch(
        "rs_workflows.dpr_flow.extract_products_and_zattrs",
        return_value=[("prod1", "s3://bucket/prod1/.zattrs")],
    )
    # Mock s3_list to avoid actual S3 calls
    mocker.patch("rs_workflows.dpr_flow.s3_list", return_value=[])

    mocker.patch(
        "rs_workflows.dpr_flow.compute_eopf_origin_datetime",
        return_value="2024-01-10T12:00:00",
    )

    # Mock read_zattrs_sync to return None, triggering the error
    mocker.patch("rs_workflows.dpr_flow.read_zattrs_sync", return_value=None)

    payload = mocker.Mock()
    payload.io.output_products = [mocker.Mock(path="s3://bucket/path")]

    with pytest.raises(RuntimeError, match="Could not read .zattrs file s3://bucket/prod1/.zattrs. Exiting."):
        update_eopf_assets.fn(
            env=env,
            input_products=[{"input": ("id", "coll")}],
            payload=payload,
            dpr_processor=DprProcessor.S1L0,
        )


def test_update_eopf_assets_skips_non_final_products(mocker):
    """
    Verify that update_eopf_assets processes only final products.
    Note: Filtering of non-final products happens in run_processor before
    calling update_eopf_assets, so this function only receives final products.
    """
    env = mocker.Mock()
    mocker.patch(
        "rs_workflows.dpr_flow.compute_eopf_origin_datetime",
        return_value="2024-01-10T12:00:00",
    )
    input_products = [{"id": "input_1"}]

    # Mock only final products (non-final products are already filtered by run_processor)
    mock_prod_final = mocker.Mock(path="s3://out/final", final_product=True)

    payload = mocker.Mock()
    payload.io.output_products = [mock_prod_final]  # Only final products

    # Mock s3_list and extract to return something for the final product
    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mocker.Mock())
    mock_s3_list = mocker.patch("rs_workflows.dpr_flow.s3_list", return_value=["s3://out/final/prod/.zattrs"])

    mocker.patch(
        "rs_workflows.dpr_flow.extract_products_and_zattrs",
        return_value=[("prod", "s3://out/final/prod/.zattrs")],
    )

    mocker.patch(
        "rs_workflows.dpr_flow.read_zattrs_sync",
        return_value={
            "stac_discovery": {
                "id": "item1",
                "properties": {"product:type": "S1_L0"},
            },
        },
    )
    mocker.patch("rs_workflows.dpr_flow.create_stac_item", return_value=[mocker.Mock()])

    # Run function
    update_eopf_assets.fn(
        env=env,
        input_products=input_products,
        payload=payload,
        dpr_processor=DprProcessor.S1L0,
    )

    # Assert s3_list was called for the final product path
    mock_s3_list.assert_called_once_with("s3://out/final")


@pytest.mark.parametrize("mocked_dpr_response", ["s3_l0"], indirect=True, ids=[""])
@pytest.mark.asyncio
async def test_run_processor_filters_non_final_products(
    mocker,
    mocked_dpr_response,  # /dpr/processes/s3_l0/execution, /dpr/jobs/{job_id}
    mocked_processor_log,
):  # pylint: disable=unused-argument
    """
    Verify that run_processor correctly filters out non-final products from
    payload.io.output_products before processing.

    This test ensures that:
    - Products with final_product=False are removed from the payload
    - Products with final_product=True are kept
    - The DPR processor is called with the filtered payload
    - update_eopf_assets receives the filtered payload
    """
    await setup_worklow_test_env()

    # # Mock environment and dependencies
    # env = mocker.Mock()
    processor = "s3_l0"
    s3_payload_run = f"s3://{MOCKED_BUCKET}/payload.yaml"
    input_products = [{"id": "input1"}]

    # Create mock products: 2 final, 1 non-final
    mock_prod_final_1 = mocker.Mock(id="product_final_1", path="s3://out/final1", final_product=True)
    mock_prod_final_2 = mocker.Mock(id="product_final_2", path="s3://out/final2", final_product=True)
    mock_prod_intermediate = mocker.Mock(id="product_intermediate", path="s3://out/intermediate", final_product=False)

    # Create payload with mixed products
    payload = mocker.Mock()
    payload.io = mocker.Mock()
    payload.io.output_products = [mock_prod_final_1, mock_prod_intermediate, mock_prod_final_2]

    # Mock Prefect logger
    mock_logger = mocker.Mock()
    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mock_logger)

    # Spy DPR client
    spy_run_process = mocker.spy(dpr_client.DprClient, "run_process")
    spy_wait_for_job = mocker.spy(dpr_client.DprClient, "wait_for_job")
    # mock_dpr_client = mocker.Mock()
    # mock_job_status = mocker.Mock()
    # mock_dpr_client.run_process.return_value = mock_job_status
    # mock_dpr_client.wait_for_job.return_value = None
    # mock_flow_env.rs_client.get_dpr_client.return_value = mock_dpr_client

    # mocker.patch("rs_workflows.dpr_flow.FlowEnv", return_value=mock_flow_env)

    # Mock record_performance_indicators
    mocker.patch("rs_workflows.dpr_flow.record_performance_indicators")

    # Mock update_eopf_assets to return mock STAC items
    items_metadata = [
        DprProcessedItemMetadata(
            stac_item=mocker.Mock(),
            product_type="S03OLCL0_",
            output_product_id="product_final_1",
        ),
        DprProcessedItemMetadata(
            stac_item=mocker.Mock(),
            product_type="S03SLSL0_",
            output_product_id="product_final_2",
        ),
    ]
    mocker.patch("rs_workflows.dpr_flow.update_eopf_assets", return_value=items_metadata)

    # Run the function
    result = await run_processor.fn(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        processor=processor,
        payload=payload,
        cluster_info=CLUSTER_INFO,
        s3_payload_run=s3_payload_run,
        input_products=input_products,
    )

    # Verify that non-final products were filtered out
    assert len(payload.io.output_products) == 2
    assert mock_prod_final_1 in payload.io.output_products
    assert mock_prod_final_2 in payload.io.output_products
    assert mock_prod_intermediate not in payload.io.output_products

    # Verify logger was called for the filtered product
    mock_logger.info.assert_any_call(
        "Output product product_intermediate is not marked as final_product, skipping catalog registration.",
    )

    # Verify DPR client was called
    # mock_dpr_client.run_process.assert_called_once()
    # mock_dpr_client.wait_for_job.assert_called_once_with(mock_job_status, mock_logger, "'s3_l0' processor")
    spy_run_process.assert_called_once()
    spy_wait_for_job.assert_called_once()

    # Verify result is the items metadata from update_eopf_assets
    assert result == items_metadata


@pytest.mark.asyncio
async def test_run_processor_raises_on_missing_io_config(mocker):
    """
    Verify that run_processor raises ValueError when payload.io is None.
    """
    env = mocker.Mock()
    processor = "s3_l0"
    cluster_info = mocker.Mock()
    s3_payload_run = "s3://bucket/payload.json"
    input_products = [{"id": "input1"}]

    # Create payload with None io
    payload = mocker.Mock()
    payload.io = None

    # Mock Prefect logger
    mocker.patch("rs_workflows.dpr_flow.get_run_logger")

    # Mock FlowEnv
    mock_flow_env = mocker.Mock()
    mock_span = mocker.Mock()
    mock_span.__enter__ = mocker.Mock(return_value=mock_span)
    mock_span.__exit__ = mocker.Mock(return_value=False)
    mock_flow_env.start_span.return_value = mock_span
    mocker.patch("rs_workflows.dpr_flow.FlowEnv", return_value=mock_flow_env)

    # Verify ValueError is raised
    with pytest.raises(ValueError, match="Payload I/O configuration is missing"):
        await run_processor.fn(
            env=env,
            processor=processor,
            payload=payload,
            cluster_info=cluster_info,
            s3_payload_run=s3_payload_run,
            input_products=input_products,
        )


def make_mock_item(origin_datetime: str, mocker):
    """Return a random product with eopf datetieme set"""
    mock_item = mocker.Mock()
    mock_item.to_dict.return_value = {
        "properties": {
            "eopf:origin_datetime": origin_datetime,
        },
    }
    return mock_item


def test_compute_eopf_origin_datetime_single_item(mocker):
    """
    Verify that compute_eopf_origin_datetime returns the correct maximum
    eopf:origin_datetime when a single CADU item is retrieved from the catalog.

    This test mocks:
    - catalog_flow.get_item.submit to return a mocked future
    - future.result() to return a mocked item with a predefined eopf:origin_datetime
    """
    env = mocker.Mock()
    env.serialize.return_value = {"env": "data"}

    input_products = [FlowInputProduct(name="input", item_id="CADU_1", collection_name="COLLECTION_1")]

    mock_item = make_mock_item("2024-01-10T12:00:00Z", mocker)

    mock_future = mocker.Mock()
    mock_future.result.return_value = mock_item

    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mocker.Mock())
    mocker.patch(
        "rs_workflows.dpr_flow.catalog_flow.get_item.submit",
        return_value=mock_future,
    )

    result = compute_eopf_origin_datetime(env, input_products)

    assert result == "2024-01-10T12:00:00+00:00"


def test_compute_eopf_origin_datetime_multiple_items_returns_max(mocker):
    """
    Verify that compute_eopf_origin_datetime returns the maximum eopf:origin_datetime
    when multiple CADU items are retrieved from the catalog.

    This test mocks:
    - catalog_flow.get_item.submit to return multiple futures
    - Each future.result() returns a mocked item with a predefined eopf:origin_datetime
    - Ensures the function computes the latest datetime correctly
    """
    env = mocker.Mock()
    env.serialize.return_value = {"env": "data"}

    input_products = [
        FlowInputProduct(name="input1", item_id="CADU_1", collection_name="COLLECTION_1"),
        FlowInputProduct(name="input2", item_id="CADU_2", collection_name="COLLECTION_2"),
    ]
    item_1 = make_mock_item("2024-01-01T00:00:00Z", mocker)
    item_2 = make_mock_item("2024-02-01T00:00:00Z", mocker)

    future_1 = mocker.Mock()
    future_1.result.return_value = item_1

    future_2 = mocker.Mock()
    future_2.result.return_value = item_2

    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mocker.Mock())
    mocker.patch(
        "rs_workflows.dpr_flow.catalog_flow.get_item.submit",
        side_effect=[future_1, future_2],
    )

    result = compute_eopf_origin_datetime(env, input_products)
    assert result == "2024-02-01T00:00:00+00:00"


def test_compute_eopf_origin_datetime_raises_on_missing_item(mocker):
    """
    Verify that compute_eopf_origin_datetime raises RuntimeError when an
    input item cannot be found in the catalog.
    """
    env = mocker.Mock()
    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mocker.Mock())

    # Mock catalog_flow.get_item.submit to return a future whose result() is None
    mock_future = mocker.Mock()
    mock_future.result.return_value = None
    mocker.patch("rs_workflows.dpr_flow.catalog_flow.get_item.submit", return_value=mock_future)

    input_products = [FlowInputProduct(name="input", item_id="missing_id", collection_name="some_coll")]

    with pytest.raises(RuntimeError, match="No valid items found to compute eopf:origin_datetime") as excinfo:
        compute_eopf_origin_datetime(env, input_products)

    # Verify the original cause contains the specific message requested by the user
    assert "Expected valid input product item missing_id was not found" in str(excinfo.value.__cause__)


def test_compute_eopf_origin_datetime_raises_on_empty_input(mocker):
    """
    Verify that compute_eopf_origin_datetime raises RuntimeError when
    input_products is empty.
    """
    env = mocker.Mock()
    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mocker.Mock())

    with pytest.raises(RuntimeError, match="No valid input products found to compute eopf:origin_datetime"):
        compute_eopf_origin_datetime(env, [])


def test_compute_eopf_origin_datetime_raises_on_catalog_error(mocker):
    """
    Verify that compute_eopf_origin_datetime raises RuntimeError
    when retrieving CADU items from the catalog fails.
    """
    env = mocker.Mock()
    env.serialize.return_value = {"env": "data"}

    input_products = [FlowInputProduct(name="input", item_id="CADU_FAIL", collection_name="COLLECTION_FAIL")]

    mocker.patch(
        "rs_workflows.dpr_flow.get_run_logger",
        return_value=mocker.Mock(),
    )

    mocker.patch(
        "rs_workflows.dpr_flow.catalog_flow.get_item.submit",
        side_effect=RuntimeError("Catalog unavailable"),
    )

    with pytest.raises(
        RuntimeError,
        match="No valid items found to compute eopf:origin_datetime",
    ):
        compute_eopf_origin_datetime(env, input_products)


def test_no_eopf_origin_datetime(mocker):
    """
    Verify that compute_eopf_origin_datetime throws a ValueError when
    none of the items has eopf:origin_datetime field.

    This test mocks:
    - catalog_flow.get_item.submit to return a mocked future
    - future.result() to return a mocked item with a predefined eopf:origin_datetime = ''
    """
    env = mocker.Mock()
    env.serialize.return_value = {"env": "data"}

    input_products = [FlowInputProduct(name="input", item_id="CADU_1", collection_name="COLLECTION_1")]

    mock_item = make_mock_item("", mocker)

    mock_future = mocker.Mock()
    mock_future.result.return_value = mock_item

    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mocker.Mock())
    mocker.patch(
        "rs_workflows.dpr_flow.catalog_flow.get_item.submit",
        return_value=mock_future,
    )

    with pytest.raises(
        ValueError,
        match="Maximum eopf datetime could not be computed",
    ):
        compute_eopf_origin_datetime(env, input_products)


def test_clean_paths_removes_existing_directories(mocker, tmp_path):
    """
    Verify that clean_paths calls shutil.rmtree for each path that exists
    and is a directory, and logs an info message for each removal.
    """

    dir_a = tmp_path / "dir_a"
    dir_b = tmp_path / "dir_b"
    dir_a.mkdir()
    dir_b.mkdir()

    mock_logger = mocker.Mock()
    mock_rmtree = mocker.patch("rs_workflows.dpr_flow.shutil.rmtree")

    clean_paths([str(dir_a), str(dir_b)], mock_logger)

    assert mock_rmtree.call_count == 2
    mock_rmtree.assert_any_call(str(dir_a))
    mock_rmtree.assert_any_call(str(dir_b))
    # One info call per directory removed
    assert mock_logger.info.call_count >= 2


def test_clean_paths_warns_and_skips_nonexistent_path(mocker, tmp_path):
    """
    Verify that clean_paths emits a warning and skips paths that do not exist,
    without raising an exception.
    """

    nonexistent = str(tmp_path / "does_not_exist")
    mock_logger = mocker.Mock()
    mock_rmtree = mocker.patch("rs_workflows.dpr_flow.shutil.rmtree")

    clean_paths([nonexistent], mock_logger)

    mock_rmtree.assert_not_called()
    mock_logger.warning.assert_called_once()
    warning_msg = mock_logger.warning.call_args[0][0]
    assert "does not exist" in warning_msg
    assert nonexistent in warning_msg


def test_clean_paths_warns_and_skips_file_path(mocker, tmp_path):
    """
    Verify that clean_paths emits a warning and skips paths that point to a
    file rather than a directory (only directories are valid autoclean targets).
    """

    file_path = tmp_path / "not_a_dir.txt"
    file_path.write_text("content")

    mock_logger = mocker.Mock()
    mock_rmtree = mocker.patch("rs_workflows.dpr_flow.shutil.rmtree")

    clean_paths([str(file_path)], mock_logger)

    mock_rmtree.assert_not_called()
    mock_logger.warning.assert_called_once()
    warning_msg = mock_logger.warning.call_args[0][0]
    assert "expected directory but found file" in warning_msg


def test_clean_paths_warns_on_rmtree_exception(mocker, tmp_path):
    """
    Verify that clean_paths catches exceptions raised by shutil.rmtree,
    logs a warning instead of propagating the error, and continues processing
    subsequent paths.
    """

    dir_a = tmp_path / "dir_a"
    dir_b = tmp_path / "dir_b"
    dir_a.mkdir()
    dir_b.mkdir()

    mock_logger = mocker.Mock()
    mocker.patch(
        "rs_workflows.dpr_flow.shutil.rmtree",
        side_effect=[OSError("Permission denied"), None],
    )

    # Should not raise; exception is caught and warned
    clean_paths([str(dir_a), str(dir_b)], mock_logger)

    mock_logger.warning.assert_called_once()
    warning_msg = mock_logger.warning.call_args[0][0]
    assert "Autoclean failed" in warning_msg
    assert str(dir_a) in warning_msg
