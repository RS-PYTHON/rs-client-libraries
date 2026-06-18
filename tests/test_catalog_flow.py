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
from unittest.mock import AsyncMock, MagicMock

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
async def test_publish_tempfixes(mocker, monkeypatch, mocked_rspy_landing_pages):  # pylint: disable=unused-argument
    """Test the temporary fixes in the publish function."""
    env = FlowEnvArgs(owner_id=OWNER_ID)

    # Mock FlowEnv to avoid Prefect block loading
    mock_logger = MagicMock()
    mocker.patch(
        "rs_workflows.catalog_flow.get_run_logger",
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

    # Mock check_and_create_collection to avoid unmocked HTTP calls
    monkeypatch.setattr(
        catalog_flow,
        "check_and_create_collection",
        AsyncMock(),
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

    await catalog_flow.publish.fn(env, catalog_mapping, items_metadata_1)

    # verify geometry and bbox were set to None
    _, added_collection, added_item = spy_add_item.call_args_list[0][0]
    assert added_collection == collection_id
    assert added_item.geometry is None
    assert added_item.bbox is None

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

    await catalog_flow.publish.fn(env, catalog_mapping, items_metadata_2)
    _, _, added_item = spy_add_item.call_args_list[0][0]
    assert added_item.properties["instruments"] == ["instrument1"]

    # invalid provider roles should be filtered
    spy_add_item.reset_mock()
    items_metadata_3 = [
        DprProcessedItemMetadata(
            stac_item=Item(
                id="item1",
                properties={
                    "product:type": "S1_GRD",
                    "datetime": "2024-01-01T00:00:00Z",
                    "providers": [
                        {"name": "valid", "roles": ["producer", "host"]},
                        {"name": "invalid_role", "roles": ["invalid"]},
                        {"name": "mixed", "roles": ["processor", "bad"]},
                        {"name": "no_roles", "other": "info"},
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

    await catalog_flow.publish.fn(env, catalog_mapping, items_metadata_3)
    _, _, added_item = spy_add_item.call_args_list[0][0]
    providers = added_item.properties["providers"]
    assert len(providers) == 2
    assert providers[0]["name"] == "valid"
    assert providers[1]["name"] == "no_roles"  # No 'roles' key so it's kept
