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

"""Unit tests for catalog_flow.py"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import responses
from pystac import Item
from starlette import status

from rs_client.stac import catalog_client
from rs_workflows import catalog_flow
from rs_workflows.flow_utils import (
    DprProcessedItemMetadata,
    FlowEnvArgs,
    FlowGeneratedProduct,
)
from tests.conftest import MOCKED_RSPY_WEBSITE, OWNER_ID


@pytest.mark.asyncio
@pytest.mark.parametrize("include_valid_providers", [True, False])
async def test_publish_tempfixes(
    mocker,
    monkeypatch,
    mocked_rspy_landing_pages,
    include_valid_providers,
):  # pylint: disable=unused-argument
    """Test the temporary fixes in the publish function."""
    env = FlowEnvArgs(owner_id=OWNER_ID)

    # Mock FlowEnv to avoid Prefect block loading
    mock_logger = MagicMock()
    mocker.patch(
        "rs_workflows.catalog_flow.get_logger",
        return_value=mock_logger,
    )
    mocker.patch(
        "rs_common.geometry_fix.get_run_logger",
        return_value=mock_logger,
    )
    real_catalog_client = catalog_client.CatalogClient(
        MOCKED_RSPY_WEBSITE,
        "test-api-key",
        OWNER_ID,
    )
    flow_env_mock = MagicMock()
    flow_env_mock.rs_client.get_catalog_client.return_value = real_catalog_client
    flow_env_mock.start_span.return_value.__enter__ = lambda self: MagicMock()
    flow_env_mock.start_span.return_value.__exit__ = lambda self, *args: None
    monkeypatch.setattr(
        catalog_flow,
        "FlowEnv",
        lambda env: flow_env_mock,
    )

    # Mock add_item
    spy_add_item = mocker.spy(catalog_client.CatalogClient, "add_item")

    # Mock Catalog API calls
    collection_id = "OUTPUT_GRD_COLLECTION"
    responses.post(
        f"{MOCKED_RSPY_WEBSITE}/catalog/collections/{OWNER_ID}:{collection_id}/items",
        json={"status": status.HTTP_200_OK},
        status=status.HTTP_200_OK,
    )

    catalog_mapping = [
        FlowGeneratedProduct(name="item1", product_type="S1_GRD", collection_name=collection_id),
    ]

    # empty geometry and bbox should be set to None
    items_metadata_1 = [
        DprProcessedItemMetadata(
            stac_item=Item(
                id="item1",
                properties={
                    "product:type": "S1_GRD",
                    "datetime": "2024-01-01T00:00:00Z",
                },
                datetime=datetime(2024, 1, 1, tzinfo=timezone.utc),
                geometry=[],  # type: ignore[arg-type]
                bbox=[],  # type: ignore[arg-type]
            ),
            product_type="S1_GRD",
            output_product_id="item1",
        ),
    ]

    published_items = await catalog_flow.publish.fn(env, catalog_mapping, items_metadata_1)

    # verify geometry and bbox were set to None
    _, added_collection, added_item = spy_add_item.call_args_list[0][0]
    assert added_collection == collection_id
    assert added_item.geometry is None
    assert added_item.bbox is None
    assert published_items == [{**added_item.to_dict(), "collection": collection_id}]

    # instruments as string should be converted to list
    spy_add_item.reset_mock()
    items_metadata_2 = [
        DprProcessedItemMetadata(
            stac_item=Item(
                id="item1",
                properties={
                    "product:type": "S1_GRD",
                    "datetime": "2024-01-01T00:00:00Z",
                    "instruments": "instrument1",  # String instead of list
                },
                datetime=datetime(2024, 1, 1, tzinfo=timezone.utc),
                geometry={"type": "Point", "coordinates": [0, 0]},
                bbox=[0, 0, 0, 0],
            ),
            product_type="S1_GRD",
            output_product_id="item1",
        ),
    ]

    published_items = await catalog_flow.publish.fn(env, catalog_mapping, items_metadata_2)
    _, _, added_item = spy_add_item.call_args_list[0][0]
    assert added_item.properties["instruments"] == ["instrument1"]
    assert published_items == [{**added_item.to_dict(), "collection": collection_id}]

    # invalid provider roles should be filtered
    spy_add_item.reset_mock()
    items_metadata_3 = [
        DprProcessedItemMetadata(
            stac_item=Item(
                id="item1",
                properties={
                    "product:type": "S1_GRD",
                    "datetime": "2024-01-01T00:00:00Z",
                    "providers": (
                        [
                            {"name": "valid", "roles": ["producer", "host"]},
                            {"name": "invalid_role", "roles": ["invalid"]},
                            {"name": "mixed", "roles": ["processor", "bad"]},
                            {"name": "no_roles", "other": "info"},
                        ]
                        if include_valid_providers
                        else []
                    )
                    + [
                        {"name": [], "roles": ["processor"]},
                        {"name": [], "roles": ["producer"]},
                        {"name": "", "roles": ["producer"]},
                        {"name": None},
                        {"name": 123},
                        {"roles": ["host"]},
                    ],
                },
                datetime=datetime(2024, 1, 1, tzinfo=timezone.utc),
                geometry={"type": "Point", "coordinates": [0, 0]},
                bbox=[0, 0, 0, 0],
            ),
            product_type="S1_GRD",
            output_product_id="item1",
        ),
    ]

    published_items = await catalog_flow.publish.fn(env, catalog_mapping, items_metadata_3)
    _, _, added_item = spy_add_item.call_args_list[0][0]
    providers = added_item.properties["providers"]
    assert providers == (
        [{"name": "valid", "roles": ["producer", "host"]}, {"name": "no_roles", "other": "info"}]
        if include_valid_providers
        else []
    )
    assert published_items == [{**added_item.to_dict(), "collection": collection_id}]


@pytest.mark.asyncio
async def test_publish_continues_after_item_publish_failure(
    mocker,
    monkeypatch,
    mocked_rspy_landing_pages,
):  # pylint: disable=unused-argument
    """Test that publish continues to the next item if a catalog publish fails."""
    env = FlowEnvArgs(owner_id=OWNER_ID)

    mock_logger = MagicMock()
    mocker.patch(
        "rs_workflows.catalog_flow.get_logger",
        return_value=mock_logger,
    )
    mocker.patch(
        "rs_common.geometry_fix.get_run_logger",
        return_value=mock_logger,
    )

    real_catalog_client = catalog_client.CatalogClient(
        MOCKED_RSPY_WEBSITE,
        "test-api-key",
        OWNER_ID,
    )
    flow_env_mock = MagicMock()
    flow_env_mock.rs_client.get_catalog_client.return_value = real_catalog_client
    flow_env_mock.start_span.return_value.__enter__ = lambda self: MagicMock()
    flow_env_mock.start_span.return_value.__exit__ = lambda self, *args: None
    monkeypatch.setattr(
        catalog_flow,
        "FlowEnv",
        lambda env: flow_env_mock,
    )

    catalog_mapping = [
        FlowGeneratedProduct(name="item1", product_type="S1_GRD", collection_name="OUTPUT_GRD_COLLECTION"),
        FlowGeneratedProduct(name="item2", product_type="S1_GRD", collection_name="OUTPUT_GRD_COLLECTION"),
    ]

    item_1 = DprProcessedItemMetadata(
        stac_item=Item(
            id="item1",
            properties={
                "product:type": "S1_GRD",
                "datetime": "2024-01-01T00:00:00Z",
            },
            datetime=datetime(2024, 1, 1, tzinfo=timezone.utc),
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 0, 0],
        ),
        product_type="S1_GRD",
        output_product_id="item1",
    )
    item_2 = DprProcessedItemMetadata(
        stac_item=Item(
            id="item2",
            properties={
                "product:type": "S1_GRD",
                "datetime": "2024-01-02T00:00:00Z",
            },
            datetime=datetime(2024, 1, 2, tzinfo=timezone.utc),
            geometry={"type": "Point", "coordinates": [1, 1]},
            bbox=[1, 1, 1, 1],
        ),
        product_type="S1_GRD",
        output_product_id="item2",
    )

    def add_item_side_effect(collection_id, item, timeout=21600):
        if item.id == "item1":
            raise RuntimeError("catalog publish failed")
        response = MagicMock()
        response.status_code = status.HTTP_200_OK
        response.text = "ok"
        return response

    add_item_mock = mocker.patch.object(
        catalog_client.CatalogClient,
        "add_item",
        side_effect=add_item_side_effect,
    )

    await catalog_flow.publish.fn(env, catalog_mapping, [item_1, item_2])

    assert add_item_mock.call_count == 2
    assert add_item_mock.call_args_list[0][0][1].id == "item1"
    assert add_item_mock.call_args_list[1][0][1].id == "item2"
    mock_logger.warning.assert_called_once()
    warning_message = mock_logger.warning.call_args[0][0]
    assert "catalog publish failed" in warning_message
    assert "item1" in warning_message


@pytest.mark.asyncio
async def test_check_collections_exist(
    mocker,
    mocked_rspy_landing_pages,
):  # pylint: disable=unused-argument
    """The existing collections are accepted, the missing ones are reported and never created."""
    mocker.patch("rs_workflows.catalog_flow.get_logger", return_value=MagicMock())
    real_catalog_client = catalog_client.CatalogClient(MOCKED_RSPY_WEBSITE, "test-api-key", OWNER_ID)
    flow_env_mock = MagicMock()
    flow_env_mock.rs_client.get_catalog_client.return_value = real_catalog_client
    spy_add_collection = mocker.spy(catalog_client.CatalogClient, "add_collection")

    def collection_url(name: str) -> str:
        return f"{MOCKED_RSPY_WEBSITE}/catalog/collections/{OWNER_ID}:{name}"

    for name in ("s03sral0c", "s03olcl0_"):
        responses.get(
            collection_url(name),
            json={
                "type": "Collection",
                "stac_version": "1.0.0",
                "id": f"{OWNER_ID}_{name}",
                "description": name,
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": [],
            },
            status=status.HTTP_200_OK,
        )
    for name in ("S03SRAL0C", "s03missing"):
        responses.get(
            collection_url(name),
            json={"code": "NotFound", "description": f"Collection {name} not found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    # Case 1: all the collections exist
    await catalog_flow.check_collections_exist(flow_env_mock, ["s03sral0c", "s03olcl0_", "s03sral0c"])

    # Case 2: the missing collections are all reported, and not created
    with pytest.raises(ValueError, match=r"missing.*\['S03SRAL0C', 's03missing'\].*RS operator/administrator"):
        await catalog_flow.check_collections_exist(flow_env_mock, ["S03SRAL0C", "s03olcl0_", "s03missing"])
    spy_add_collection.assert_not_called()


def test_get_generated_product_collections():
    """The collections default to the product type in lowercase, without duplicates."""
    mapping = [
        FlowGeneratedProduct(name="S03SRAL0C", product_type="S03SRAL0C"),
        FlowGeneratedProduct(name="S03OLCL0", product_type="S03OLCL0_"),
        FlowGeneratedProduct(name="S03CACHE", product_type="*", collection_name="s3_production_cache_s3l0"),
        FlowGeneratedProduct(name="S03OLCL0_BIS", product_type="S03OLCL0_"),
    ]
    assert catalog_flow.get_generated_product_collections(mapping) == [
        "s03sral0c",
        "s03olcl0_",
        "s3_production_cache_s3l0",
    ]

    with pytest.raises(ValueError, match="cannot be '\\*' if the collection name is not specified"):
        catalog_flow.get_generated_product_collections([FlowGeneratedProduct(name="ANY", product_type="*")])
