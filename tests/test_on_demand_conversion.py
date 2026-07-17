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

"""Unit tests for the on-demand conversion flow."""

import json
from contextlib import nullcontext
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from pystac import Asset, Item

import rs_workflows.on_demand_conversion as on_demand_conversion_flow
from rs_workflows.flow_utils import ConversionIn, FlowEnvArgs, FlowGeneratedProduct


@pytest.mark.asyncio
async def test_on_demand_conversion_helpers_cover_mapping_zarr_and_safe_task(tmp_path, monkeypatch, mocker):
    """Cover helper branches used by the SAFE conversion orchestration."""
    output_product_type = "S01SIWSLC"

    # This test intentionally groups the small helper paths that are hard to
    # reach from the full orchestration test without adding a lot of setup. The
    # goal is to increase coverage while still keeping each assertion tied to a
    # real branch used by the conversion flow.

    # 1. Generated product resolution.
    # The conversion flow computes an EOPF product type from the legacy SAFE
    # product type, then resolves it against the generated product mapping sent
    # by the caller.
    wildcard_product = FlowGeneratedProduct(
        name=output_product_type,
        product_type="*",
        collection_name="SAFE_OUTPUT",
    )

    # Wildcard product type is the supported fallback when the generated product
    # name already matches the computed output product type.
    assert (
        on_demand_conversion_flow.resolve_generated_product(output_product_type, [wildcard_product]) is wildcard_product
    )

    # A wildcard type without an explicit collection cannot be resolved safely
    # because the flow would otherwise publish to the literal "*" collection.
    with pytest.raises(ValueError, match="collection_name is mandatory"):
        on_demand_conversion_flow.resolve_generated_product(
            output_product_type,
            [
                FlowGeneratedProduct(
                    name=output_product_type,
                    product_type="*",
                    collection_name=None,
                ),
            ],
        )

    with pytest.raises(ValueError, match="No generated product mapping found"):
        on_demand_conversion_flow.resolve_generated_product(
            output_product_type,
            [
                FlowGeneratedProduct(
                    name="OTHER_PRODUCT",
                    product_type="*",
                    collection_name="OTHER_COLLECTION",
                ),
            ],
        )

    # 2. Zarr STAC discovery.
    # read_zarr_stac_item reads the root .zattrs file written by EOPF and builds
    # the STAC item through the same create_stac_item helper used by DPR flows.
    zarr_dir = tmp_path / "S01SIWSLC_SAFE_CONVERTED.zarr"
    zarr_dir.mkdir()
    (zarr_dir / ".zattrs").write_text(
        json.dumps(
            {
                "stac_discovery": {
                    "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
                    "bbox": [1.0, 2.0, 1.0, 2.0],
                    "properties": {
                        "datetime": "2024-01-01T00:00:00+00:00",
                        "product:type": output_product_type,
                    },
                },
            },
        ),
        encoding="utf-8",
    )

    stac_item = on_demand_conversion_flow.read_zarr_stac_item(str(zarr_dir))

    assert stac_item.id == "S01SIWSLC_SAFE_CONVERTED"
    assert stac_item.properties["product:type"] == output_product_type
    assert stac_item.assets["S01SIWSLC_SAFE_CONVERTED"].href == str(zarr_dir)

    # Missing stac_discovery metadata is treated as a conversion output problem
    # and should fail before publication.
    with pytest.raises(RuntimeError, match="Missing 'stac_discovery' metadata"):
        invalid_zarr_dir = tmp_path / "invalid.zarr"
        invalid_zarr_dir.mkdir()
        (invalid_zarr_dir / ".zattrs").write_text("{}", encoding="utf-8")
        on_demand_conversion_flow.read_zarr_stac_item(str(invalid_zarr_dir))

    # 3. SAFE conversion task wrapper.
    # The Prefect task is exercised through .fn so no Prefect engine is started;
    # FlowEnv and the DPR client are mocked to verify only orchestration logic.
    dpr_client_mock = MagicMock()
    dpr_client_mock.run_conv_safe_zarr.return_value = {"job_id": "safe-conversion-job"}
    dpr_client_mock.wait_for_job.return_value = {"zarr_uri": "s3://bucket/output.zarr"}

    flow_env_mock = MagicMock()
    flow_env_mock.start_span.return_value = nullcontext()
    flow_env_mock.rs_client.get_dpr_client.return_value = dpr_client_mock
    monkeypatch.setattr(on_demand_conversion_flow, "FlowEnv", lambda env: flow_env_mock)
    mocker.patch.object(on_demand_conversion_flow, "get_run_logger", return_value=MagicMock())

    payload = {
        "input_safe_path": "s3://input-bucket/S1A_SAFE",
        "output_zarr_dir_path": "s3://output-bucket/test-owner/SAFE_OUTPUT",
    }
    cluster_info = on_demand_conversion_flow.ClusterInfo(
        jupyter_token="",
        dask_gateway_address="",
        cluster_label="dask-safe",
        cluster_instance="dask-instance-1",
    )

    result = await on_demand_conversion_flow.safe_conversion_task.fn(
        FlowEnvArgs(owner_id="test-owner"),
        payload,
        cluster_info,
    )

    assert result == {"zarr_uri": "s3://bucket/output.zarr"}
    dpr_client_mock.run_conv_safe_zarr.assert_called_once_with(payload, cluster_info)
    dpr_client_mock.wait_for_job.assert_called_once()


@pytest.mark.asyncio
async def test_on_demand_conversion_orchestrates_safe_conversion_happy_path(monkeypatch, mocker):
    """Validate the main SAFE conversion orchestration with external tasks mocked."""
    owner_id = "test-owner"
    safe_item_id = "S1A_IW_SLC_SAFE"
    output_product_type = "S01SIWSLC"
    output_collection = "S01SIWSLC_COLLECTION"
    serialized_env = FlowEnvArgs(owner_id=owner_id)
    generated_product = FlowGeneratedProduct(
        name=output_product_type,
        product_type=output_product_type,
        collection_name=output_collection,
    )
    stac_input = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": safe_item_id,
                "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
                "bbox": [1.0, 2.0, 1.0, 2.0],
                "properties": {
                    "datetime": "2024-01-01T00:00:00Z",
                    "product:type": "IW_SLC__1S",
                },
                "links": [
                    {
                        "rel": "self",
                        "href": "https://catalog.test/collections/safe/items/S1A_IW_SLC_SAFE",
                    },
                ],
                "assets": {
                    "product": {
                        "href": "s3://source-bucket/original/S1A_IW_SLC_SAFE",
                    },
                },
            },
        ],
    }
    conversion_input = ConversionIn(
        env=serialized_env,
        stac_input=stac_input,
        generated_product_to_collection_identifier=generated_product,
        owner_id=owner_id,
        dask_cluster_label="dask-safe",
        dask_cluster_instance="dask-instance-1",
    )

    # The catalog returns the item after staging. Its asset href intentionally
    # points below "/{item_id}/" because the flow trims it back to the SAFE root.
    staged_safe_item = Item(
        id=safe_item_id,
        geometry={"type": "Polygon", "coordinates": [[[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]]},
        bbox=[1, 2, 3, 4],
        datetime=datetime(2024, 1, 1, tzinfo=timezone.utc),
        properties={"product:type": "IW_SLC__1S"},
    )
    staged_safe_item.add_asset(
        "product",
        Asset(href=f"s3://staged-bucket/work/{safe_item_id}/manifest.safe"),
    )

    converted_item = Item(
        id="converted-product",
        geometry=None,
        bbox=None,
        datetime=datetime(2024, 1, 1, tzinfo=timezone.utc),
        properties={
            "datetime": "2024-01-01T00:00:00Z",
            "product:type": "SHOULD_BE_OVERWRITTEN",
        },
    )
    converted_item.add_asset("converted-product", Asset(href="s3://output-bucket/zarr/converted-product.zarr"))

    mocker.patch.object(on_demand_conversion_flow, "get_run_logger", return_value=MagicMock())
    monkeypatch.setenv("JUPYTERHUB_API_TOKEN", "")
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "")
    monkeypatch.setenv("RSPY_HOST_OSAM", "https://osam.test")

    flow_env_mock = MagicMock()
    flow_env_mock.serialize.return_value = serialized_env
    flow_env_mock.start_span.return_value = nullcontext()
    catalog_client_mock = MagicMock()
    catalog_client_mock.get_items.return_value = [staged_safe_item]
    flow_env_mock.rs_client.get_catalog_client.return_value = catalog_client_mock
    monkeypatch.setattr(on_demand_conversion_flow, "FlowEnv", lambda env: flow_env_mock)

    staging_future = MagicMock()
    staging_future.result.return_value = {"stage-safe": {"status": "successful"}}
    staging_task_mock = MagicMock()
    staging_task_mock.submit.return_value = staging_future
    staging_with_options_mock = mocker.patch.object(
        on_demand_conversion_flow.staging_task,
        "with_options",
        return_value=staging_task_mock,
    )

    fetch_csv_mock = mocker.patch.object(
        on_demand_conversion_flow,
        "fetch_csv_from_endpoint",
        return_value=[["*", "*", "*", "*", "output-bucket"]],
    )
    find_bucket_mock = mocker.patch.object(
        on_demand_conversion_flow,
        "find_s3_output_bucket",
        return_value="output-bucket",
    )
    find_product_type_mock = mocker.patch.object(
        on_demand_conversion_flow,
        "find_product_type",
        return_value={"productType": output_product_type},
    )
    read_zarr_mock = mocker.patch.object(
        on_demand_conversion_flow,
        "read_zarr_stac_item",
        return_value=converted_item,
    )

    conversion_future = MagicMock()
    conversion_future.result.return_value = {"zarr_uri": "s3://output-bucket/test-owner/S01SIWSLC_COLLECTION/out.zarr"}
    safe_conversion_submit_mock = mocker.patch.object(
        on_demand_conversion_flow.safe_conversion_task,
        "submit",
        return_value=conversion_future,
    )

    publish_future = MagicMock()
    publish_future.result.return_value = None
    publish_submit_mock = mocker.patch.object(
        on_demand_conversion_flow.catalog_flow.publish,
        "submit",
        return_value=publish_future,
    )
    cleanup_future = MagicMock()
    cleanup_future.result.return_value = None
    cleanup_submit_mock = mocker.patch.object(
        on_demand_conversion_flow.cleanup_staged_safe_item_task,
        "submit",
        return_value=cleanup_future,
    )

    await on_demand_conversion_flow.on_demand_conversion.fn(conversion_input)

    staging_with_options_mock.assert_called_once()
    staging_task_mock.submit.assert_called_once_with(
        serialized_env,
        stac_input=stac_input,
        catalog_collection_identifier=output_collection,
        asset_names={"product"},
        poll_interval=10,
    )
    catalog_client_mock.get_items.assert_called_once_with(
        collection_id=output_collection,
        items_ids=[safe_item_id],
    )

    find_product_type_mock.assert_called_once_with("IW_SLC__1S")
    fetch_csv_mock.assert_called_once_with("https://osam.test/internal/configuration")
    find_bucket_mock.assert_called_once_with(
        [["*", "*", "*", "*", "output-bucket"]],
        owner_id,
        output_collection,
        output_product_type,
    )

    _, payload, cluster_info = safe_conversion_submit_mock.call_args.args
    assert payload == {
        "input_safe_path": f"s3://staged-bucket/work/{safe_item_id}",
        "output_zarr_dir_path": f"s3://output-bucket/{owner_id}/{output_collection}",
    }
    assert cluster_info.jupyter_token == ""
    assert cluster_info.dask_gateway_address == ""
    assert cluster_info.cluster_label == "dask-safe"
    assert cluster_info.cluster_instance == "dask-instance-1"

    read_zarr_mock.assert_called_once_with("s3://output-bucket/test-owner/S01SIWSLC_COLLECTION/out.zarr")
    assert converted_item.properties["product:type"] == output_product_type
    assert converted_item.geometry == staged_safe_item.geometry
    assert converted_item.bbox == staged_safe_item.bbox

    derived_from_links = converted_item.get_links("derived_from")
    assert len(derived_from_links) == 1
    assert derived_from_links[0].target == "https://catalog.test/collections/safe/items/S1A_IW_SLC_SAFE"

    publish_submit_mock.assert_called_once()
    published_env, published_mapping, published_items = publish_submit_mock.call_args.args
    assert published_env == serialized_env
    assert published_mapping == [generated_product]
    assert published_items[0].output_product_id == output_product_type
    assert published_items[0].product_type == output_product_type
    assert published_items[0].stac_item is converted_item
    publish_future.result.assert_called_once()

    cleanup_submit_mock.assert_called_once_with(serialized_env, output_collection, safe_item_id)
    cleanup_future.result.assert_called_once()


async def test_on_demand_conversion_raises_when_staging_produces_no_item(monkeypatch, mocker):
    """A 'successful' staging that stages nothing fails with a clear error, not IndexError."""
    owner_id = "test-owner"
    conversion_input = ConversionIn(
        env=FlowEnvArgs(owner_id=owner_id),
        stac_input="https://catalog.test/collections/safe/items/S1A_IW_SLC_SAFE",
        generated_product_to_collection_identifier=FlowGeneratedProduct(
            name="S01SIWSLC",
            product_type="S01SIWSLC",
            collection_name="s01siwslc",
        ),
        owner_id=owner_id,
        dask_cluster_label="dask-safe",
        dask_cluster_instance="dask-instance-1",
    )

    mocker.patch.object(on_demand_conversion_flow, "get_run_logger", return_value=MagicMock())

    flow_env_mock = MagicMock()
    flow_env_mock.serialize.return_value = FlowEnvArgs(owner_id=owner_id)
    flow_env_mock.start_span.return_value = nullcontext()
    catalog_client_mock = MagicMock()
    catalog_client_mock.get_items.return_value = []  # staging staged nothing
    flow_env_mock.rs_client.get_catalog_client.return_value = catalog_client_mock
    monkeypatch.setattr(on_demand_conversion_flow, "FlowEnv", lambda env: flow_env_mock)

    # Staging reports success even though no item was staged.
    staging_future = MagicMock()
    staging_future.result.return_value = {"stage-safe": {"status": "successful"}}
    staging_task_mock = MagicMock()
    staging_task_mock.submit.return_value = staging_future
    mocker.patch.object(
        on_demand_conversion_flow.staging_task,
        "with_options",
        return_value=staging_task_mock,
    )

    with pytest.raises(RuntimeError, match="Staging produced no catalog item"):
        await on_demand_conversion_flow.on_demand_conversion.fn(conversion_input)
