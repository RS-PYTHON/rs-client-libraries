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
"""test of helper functions for dpr_flow.py"""
import datetime
import json
from pathlib import Path

import pytest

from rs_client.ogcapi.dpr_client import DprProcessor
from rs_workflows.dpr_flow import (
    compute_eopf_origin_datetime,
    create_stac_item,
    read_zattrs_sync,
    s3_download_file_sync,
    s3_list,
    update_eopf_assets,
)


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
    - compute_eopf_origin_datetime is called to populate eopf:origin_datetime
    - Each feature results in one Item being created
    - STAC properties like stac_version are correctly set
    - Assets are built with correct href, title, media_type, and extra_fields
    - Assets are attached to the corresponding Item
    """
    env = mocker.Mock()
    input_products = [
        {"dummy": "input"},
    ]

    eopf_feature = {
        "id": "feature_1",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [0, 0, 0, 0],
        "properties": {
            "datetime": "2024-01-01T00:00:00",
        },
    }

    mocker.patch(
        "rs_workflows.dpr_flow.compute_eopf_origin_datetime",
        return_value="2024-01-10T12:00:00",
    )

    mock_item_cls = mocker.patch("rs_workflows.dpr_flow.Item")
    mock_asset_cls = mocker.patch("rs_workflows.dpr_flow.Asset")

    mock_item = mocker.Mock()
    mock_item.id = "feature_1"
    mock_item.assets = {}

    mock_item_cls.return_value = mock_item

    item = create_stac_item(
        env=env,
        input_products=input_products,
        eopf_feature=eopf_feature,
        s3_data_location="s3://my-bucket/output/feature_1.zarr",
        product_name="feature_1.zarr",
        dpr_processor=DprProcessor.S1L0,
    )

    # compute_eopf_origin_datetime called correctly
    compute_call = mocker.patch("rs_workflows.dpr_flow.compute_eopf_origin_datetime")
    compute_call.assert_not_called()  # sanity: patched above

    first_call_kwargs = mock_item_cls.call_args_list[0].kwargs
    assert first_call_kwargs["id"] == "feature_1.zarr"
    assert isinstance(first_call_kwargs["datetime"], datetime.datetime)
    assert first_call_kwargs["properties"]["eopf:origin_datetime"] == "2024-01-10T12:00:00"
    assert first_call_kwargs["properties"]["stac_version"] == "1.1.0"

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

    # Assets attached to item
    assert "feature_1.zarr" in item.assets


def test_update_eopf_assets_happy_path(mocker):
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

    input_products = [
        {"id": "input_1"},
    ]

    payload = mocker.Mock()
    payload.io.output_products = [
        mocker.Mock(path="s3://my-bucket/output/"),
    ]

    all_files = [
        "s3://my-bucket/output/product_a/.zattrs",
    ]

    product = "product_a"
    zattrs = "s3://my-bucket/output/product_a/.zattrs"

    zattrs_data = {
        "stac_discovery": {
            "id": "feature_1",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "bbox": [0, 0, 0, 0],
            "properties": {
                "datetime": "2024-01-01T00:00:00",
                "product:type": "EOPF_TYPE_A",
            },
        },
    }

    eopf_items = [zattrs_data["stac_discovery"]]  # type: ignore
    eopf_types = ["EOPF_TYPE_A"]

    mock_stac_items = [mocker.Mock()]

    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mocker.Mock())

    mock_s3_list = mocker.patch(
        "rs_workflows.dpr_flow.s3_list",
        return_value=all_files,
    )

    mock_extract = mocker.patch(
        "rs_workflows.dpr_flow.extract_products_and_zattrs",
        return_value=[(product, zattrs)],
    )

    mock_read = mocker.patch(
        "rs_workflows.dpr_flow.read_zattrs_sync",
        return_value=zattrs_data,
    )

    mock_create = mocker.patch(
        "rs_workflows.dpr_flow.create_stac_item",
        return_value=mock_stac_items,
    )

    stac_items, result_eopf_types = update_eopf_assets.fn(
        env=env,
        input_products=input_products,
        payload=payload,
        dpr_processor=DprProcessor.S1L0,
    )

    assert stac_items[0] == mock_stac_items
    assert result_eopf_types == eopf_types

    mock_s3_list.assert_called_once_with("s3://my-bucket/output/")

    mock_extract.assert_called_once_with(
        all_files,
        "s3://my-bucket/output/",
    )

    mock_read.assert_called_once_with(zattrs)

    mock_create.assert_called_once_with(
        env,
        input_products,
        eopf_items[0],
        "s3://my-bucket/output/product_a/.zattrs",
        "product_a",
        DprProcessor.S1L0,
    )


def test_update_eopf_assets_skips_non_final_products(mocker):
    """
    Verify that update_eopf_assets skips processing for products where
    final_product is False.
    """
    env = mocker.Mock()
    input_products = [{"id": "input_1"}]

    # Mock products: one final, one intermediate (not final)
    mock_prod_final = mocker.Mock(path="s3://out/final", final_product=True)
    mock_prod_intermediate = mocker.Mock(path="s3://out/intermediate", final_product=False)

    payload = mocker.Mock()
    payload.io.output_products = [mock_prod_final, mock_prod_intermediate]

    # Mock s3_list and extract to return something only for the final product
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

    # Assert s3_list was called ONLY for the final product path
    mock_s3_list.assert_called_once_with("s3://out/final")

    # Assert s3_list was NOT called for intermediate product path
    assert mocker.call("s3://out/intermediate") not in mock_s3_list.mock_calls


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

    input_products = [
        {"input": ("CADU_1", "COLLECTION_1")},
    ]

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
        {"input1": ("CADU_1", "COLLECTION_1")},
        {"input2": ("CADU_2", "COLLECTION_2")},
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


def test_compute_eopf_origin_datetime_raises_on_catalog_error(mocker):
    """
    Verify that compute_eopf_origin_datetime raises RuntimeError
    when retrieving CADU items from the catalog fails.
    """
    env = mocker.Mock()
    env.serialize.return_value = {"env": "data"}

    input_products = [
        {"input": ("CADU_FAIL", "COLLECTION_FAIL")},
    ]

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
