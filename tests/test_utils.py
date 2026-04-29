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

"""Unit tests for utility funtions."""

# pylint: disable=redefined-outer-name,unused-argument

import json
import tarfile
import zipfile
from contextlib import suppress
from datetime import datetime

import pytest
import requests  # type: ignore
import responses
from prefect.blocks.system import Secret
from pydantic import SecretStr

from rs_common import prefect_utils
from rs_common.utils import (
    _extract_nested_archive,
    _is_safe_extract_path,
    create_valcover_filter,
    env_bool,
    extract_tar,
    extract_zip,
    get_href_service,
    get_upload_prefix,
    normalize_extract_dir,
    read_response_error,
    recursive_extract,
    strftime_millis,
    strip_archive_suffix,
)
from rs_workflows.catalog_flow import resolve_collection
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessedItemMetadata,
    FlowGeneratedProduct,
    FlowInputProduct,
)
from tests.conftest import (
    MOCKED_RSPY_WEBSITE,
    OWNER_ID,
    S3_ACCESSKEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRETKEY,
)

RSPY_APIKEY = "RSPY_APIKEY"


@responses.activate
def test_response_error():
    """Test reading responses errors."""

    dummy_href = "https://DUMMY_HREF"
    detail = "detail message"
    error = "error message"
    content = "response content"
    timeout = 10  # seconds

    responses.get(url=dummy_href, status=500, json={"detail": detail})
    assert read_response_error(requests.get(dummy_href, timeout=timeout)) == detail

    responses.get(url=dummy_href, status=500, json={"error": error})
    assert read_response_error(requests.get(dummy_href, timeout=timeout)) == error

    responses.get(url=dummy_href, status=500, body=content)
    assert read_response_error(requests.get(dummy_href, timeout=timeout)) == content


def test_get_href_service(
    set_db_env_var,  # pylint: disable=unused-argument
):
    """Test the get_href_service function."""

    rs_server_href = "https://dummy-rs-server-href/endpoint/"
    assert get_href_service(rs_server_href, "RSPY_HOST_CATALOG") == "https://dummy-catalog/catalog"
    assert get_href_service(rs_server_href, "RSPY_HOST_CADIP") == "https://dummy-cadip/cadip"
    assert get_href_service(rs_server_href, "RSPY_HOST_AUXIP") == "https://dummy-auxip/auxip"
    assert get_href_service(rs_server_href, "RSPY_HOST_PRIP") == "https://dummy-prip/prip"
    assert get_href_service(rs_server_href, "RSPY_HOST_STAGING") == "https://dummy-staging/staging"
    assert get_href_service(rs_server_href, "RSPY_HOST_UNKNWON") == rs_server_href.rstrip("/")


def test_resolve_collection_generated_product(mocker):
    """Check resolve_collection works with a list of FlowGeneratedProduct instances."""
    mocker.patch("rs_workflows.catalog_flow.get_run_logger")

    input_collections: list[FlowGeneratedProduct] = [
        FlowGeneratedProduct(name="product_name_1", product_type="product_type_1", collection_name="collection_1"),
        FlowGeneratedProduct(name="product_name_2", product_type="product_type_2", collection_name="collection_2"),
        FlowGeneratedProduct(name="product_name_3", product_type="*", collection_name="collection_3"),
        FlowGeneratedProduct(name="S01GPSRAW", product_type="*", collection_name="collection_GPS"),
        FlowGeneratedProduct(name="S01HKMRAW", product_type="*", collection_name="collection_HKM"),
    ]

    # Exact match
    meta_1 = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="product_type_1",
        output_product_id="product_name_1",
    )
    assert resolve_collection(meta_1, input_collections) == "collection_1"

    # Match with wildcard type
    meta_3 = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="any_type",
        output_product_id="product_name_3",
    )
    assert resolve_collection(meta_3, input_collections) == "collection_3"

    # According to user story 986, the wrong collection was picked (HKM instead of GPS)
    meta_gps = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="S01GPSRAW",
        output_product_id="S01GPSRAW",
    )
    assert resolve_collection(meta_gps, input_collections) == "collection_GPS"

    # According to user story 986, the wrong collection was picked (HKM instead of GPS)
    meta_hkm = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="S01HKMRAW",
        output_product_id="S01HKMRAW",
    )
    assert resolve_collection(meta_hkm, input_collections) == "collection_HKM"

    # Wildcard name is NOT supported
    input_wildcard_name: list[FlowGeneratedProduct] = [
        FlowGeneratedProduct(name="*", product_type="product_type_4", collection_name="collection_4"),
    ]
    meta_4 = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="product_type_4",
        output_product_id="any_name",
    )
    with pytest.raises(ValueError):
        resolve_collection(meta_4, input_wildcard_name)

    # Priority: Exact type should be picked even if a wildcard match exists earlier for the same name
    priority_collections: list[FlowGeneratedProduct] = [
        FlowGeneratedProduct(name="exact_name", product_type="*", collection_name="wildcard_type"),
        FlowGeneratedProduct(name="exact_name", product_type="exact_type", collection_name="exact_match"),
    ]
    meta_priority = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="exact_type",
        output_product_id="exact_name",
    )
    assert resolve_collection(meta_priority, priority_collections) == "exact_match"

    # Protection: product_type="*" requires collection_name to be specified
    invalid_wildcard: list[FlowGeneratedProduct] = [
        FlowGeneratedProduct(name="product_name", product_type="*", collection_name=None),
    ]
    meta_wildcard = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="any_type",
        output_product_id="product_name",
    )
    with pytest.raises(RuntimeError, match=r"cannot be '\*' if the collection name is not specified"):
        resolve_collection(meta_wildcard, invalid_wildcard)


async def setup_worklow_test_env(env_vars: dict[str, str] | None = None):
    """Set up secret blocks needed for correct execution of workflows in Prefect"""
    # Environment variables for all users. For these test we don't need specific values
    # so it creates an empty secret. See test_prefect_utils.py for a real case example.
    # Use an empty dictionary if input_dict is None
    # Default arguments are evaluated once when the function is defined, not each
    # time the function is called. If env_vars = {} would have been used and modify env_vars in one call,
    # this modified dictionary would persists for subsequent calls, which can lead to bugs.
    # Using env_vars = None and creating a new empty dictionary inside this function avoids this issue.
    env_vars = env_vars if env_vars is not None else {}
    # Serialize dictionary to a JSON string and wrap it in SecretStr
    secret_value = SecretStr(json.dumps(env_vars))

    # Remove the existing blocks, if any
    user_block_name = prefect_utils.format_env_user(prefect_utils.BLOCK_NAME_ENV_USER, OWNER_ID)
    with suppress(ValueError):
        await Secret.delete(prefect_utils.BLOCK_NAME_ENV_GLOBAL)
    with suppress(ValueError):
        await Secret.delete(user_block_name)

    await Secret(
        value=secret_value,
    ).save(  # type: ignore[arg-type]
        prefect_utils.BLOCK_NAME_ENV_GLOBAL,
        overwrite=True,
    )

    # Create prefect block for current user
    await Secret(
        value={  # type: ignore[arg-type]
            "RSPY_WEBSITE": MOCKED_RSPY_WEBSITE,
            "RSPY_APIKEY": RSPY_APIKEY,
            "S3_ACCESSKEY": S3_ACCESSKEY,
            "S3_SECRETKEY": S3_SECRETKEY,
            "S3_REGION": S3_REGION,
            "S3_ENDPOINT": S3_ENDPOINT,
        },
    ).save(user_block_name, overwrite=True)


def test_flow_input_product_items():
    """Test that the items method of FlowInputProduct returns the correct items."""
    product = FlowInputProduct(
        name="input1",
        item_id="session123",
        collection_name="collectionA",
    )

    items = dict(product.items())

    assert items["name"] == "input1"
    assert items["item_id"] == "session123"
    assert items["collection_name"] == "collectionA"


def test_flow_generated_product_items_with_collection():
    """Test that the items() of FlowGeneratedProduct returns the correct items when collection_name is provided."""
    product = FlowGeneratedProduct(
        name="output1",
        product_type="TYPE_A",
        collection_name="collectionB",
    )

    items = dict(product.items())

    assert items["name"] == "output1"
    assert items["product_type"] == "TYPE_A"
    assert items["collection_name"] == "collectionB"


def test_flow_generated_product_items_without_collection():
    """Test that the items() of FlowGeneratedProduct returns the correct items when collection_name is not provided."""
    product = FlowGeneratedProduct(
        name="output2",
        product_type="TYPE_B",
    )

    items = dict(product.items())

    assert items["name"] == "output2"
    assert items["product_type"] == "TYPE_B"
    assert items["collection_name"] is None


def test_auxiliary_product_mapping_items():
    """Test that the items() of AuxiliaryProductMapping returns the correct items."""
    mapping = AuxiliaryProductMapping(
        product_type="*",
        collection_name="aux_collection",
    )

    items = dict(mapping.items())

    assert items["product_type"] == "*"
    assert items["collection_name"] == "aux_collection"


@pytest.fixture
def mock_utils_logger(monkeypatch, mocker):
    """Replace Prefect run logger with a plain mock for utility tests."""
    logger = mocker.Mock()
    monkeypatch.setattr("rs_common.utils.get_run_logger", lambda: logger)
    return logger


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        ("yes", False, True),
        ("0", True, False),
        ("maybe", True, True),
        ("maybe", False, False),
    ],
)
def test_env_bool(monkeypatch, value, default, expected):
    """Test env_bool parsing and fallback behavior."""
    monkeypatch.setenv("TEST_BOOL", value)
    assert env_bool("TEST_BOOL", default) is expected


def test_get_href_service_raises_without_rs_server_href(monkeypatch):
    """Test get_href_service raises when no env override or RS server href is provided."""
    monkeypatch.delenv("RSPY_HOST_NOT_SET", raising=False)
    with pytest.raises(RuntimeError, match="RS-Server URL is undefined"):
        get_href_service(None, "RSPY_HOST_NOT_SET")


def test_strftime_millis():
    """Test datetime formatting with millisecond precision."""
    assert strftime_millis(datetime(2024, 1, 2, 3, 4, 5, 678901)) == "2024-01-02T03:04:05.678Z"


def test_create_valcover_filter_with_datetimes():
    """Test ValCover filter creation from datetime inputs."""
    start = datetime(2024, 1, 2, 3, 4, 5, 123000)
    end = datetime(2024, 1, 2, 4, 5, 6, 456000)
    result = create_valcover_filter(start, end, "AUX_TEST")
    assert result == {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "product:type"}, "AUX_TEST"]},
            {
                "op": "t_contains",
                "args": [
                    {"interval": [{"property": "start_datetime"}, {"property": "end_datetime"}]},
                    {"interval": ["2024-01-02T03:04:05.123Z", "2024-01-02T04:05:06.456Z"]},
                ],
            },
        ],
    }


@pytest.mark.parametrize(
    ("member_name", "expected"),
    [
        ("safe/file.txt", True),
        ("nested\\safe.txt", True),
        ("../escape.txt", False),
        ("/absolute.txt", False),
    ],
)
def test_is_safe_extract_path(tmp_path, member_name, expected):
    """Test archive path safety checks."""
    assert _is_safe_extract_path(tmp_path, member_name) is expected


def test_extract_zip_skips_unsafe_members(tmp_path, mock_utils_logger):
    """Test ZIP extraction keeps safe members and skips unsafe ones."""
    zip_path = tmp_path / "archive.zip"
    extract_dir = tmp_path / "out"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("safe/file.txt", "ok")
        archive.writestr("../escape.txt", "bad")

    extract_zip(zip_path, extract_dir)

    assert (extract_dir / "safe" / "file.txt").read_text() == "ok"
    assert not (tmp_path / "escape.txt").exists()
    mock_utils_logger.warning.assert_called_with("Skipping unsafe ZIP member: ../escape.txt")


def test_extract_tar_skips_unsafe_members(tmp_path, mock_utils_logger):
    """Test TAR extraction keeps safe members and skips unsafe ones."""
    tar_path = tmp_path / "archive.tar"
    extract_dir = tmp_path / "out"
    safe_file = tmp_path / "safe.txt"
    unsafe_file = tmp_path / "unsafe.txt"
    safe_file.write_text("ok")
    unsafe_file.write_text("bad")

    with tarfile.open(tar_path, "w") as archive:
        archive.add(safe_file, arcname="safe/file.txt")
        archive.add(unsafe_file, arcname="../escape.txt")

    assert extract_tar(tar_path, extract_dir) == 1
    assert (extract_dir / "safe" / "file.txt").read_text() == "ok"
    assert not (tmp_path / "escape.txt").exists()
    mock_utils_logger.warning.assert_called_with("Skipping unsafe TAR member: ../escape.txt")


def test_strip_archive_suffix():
    """Test supported archive suffixes are removed correctly."""
    assert strip_archive_suffix("file.zip") == "file"
    assert strip_archive_suffix("file.tar") == "file"
    assert strip_archive_suffix("file.tgz") == "file"
    assert strip_archive_suffix("file.TGZ") == "file"
    assert strip_archive_suffix("file.tar.gz") == "file"
    assert strip_archive_suffix("file.raw") == "file.raw"


def test_get_upload_prefix():
    """Test upload prefix normalization for ZIP and TAR-like assets."""
    zip_href = "s3://bucket/user/item/file.zip"
    tar_href = "s3://bucket/user/item.tar/file.tar"
    plain_href = "s3://bucket/user/item/file.tar"
    uppercase_tgz_href = "s3://bucket/user/item.TGZ/file.TGZ"

    assert get_upload_prefix(zip_href, "file.zip") == "s3://bucket/user/item/"
    assert get_upload_prefix(tar_href, "file.tar") == "s3://bucket/user/item/"
    assert get_upload_prefix(plain_href, "file.tar") == "s3://bucket/user/item/"
    assert get_upload_prefix(uppercase_tgz_href, "file.TGZ") == "s3://bucket/user/item/"


def test_extract_nested_archive(tmp_path, mock_utils_logger):
    """Test nested archive extraction removes the archive on success."""
    nested_tar = tmp_path / "nested.tar"
    content = tmp_path / "content.txt"
    content.write_text("ok")

    with tarfile.open(nested_tar, "w") as archive:
        archive.add(content, arcname="inside.txt")

    assert _extract_nested_archive(nested_tar) is True
    assert (tmp_path / "inside.txt").read_text() == "ok"
    assert not nested_tar.exists()


def test_recursive_extract_processes_nested_archives(tmp_path, mock_utils_logger):
    """Test recursive extraction handles nested TAR archives."""
    outer_dir = tmp_path / "folder"
    outer_dir.mkdir()
    inner_tar = outer_dir / "inner.tar"
    payload = tmp_path / "payload.txt"
    payload.write_text("ok")

    with tarfile.open(inner_tar, "w") as archive:
        archive.add(payload, arcname="payload.txt")

    assert recursive_extract(tmp_path) == 1
    assert (outer_dir / "payload.txt").read_text() == "ok"
    assert not inner_tar.exists()


def test_normalize_extract_dir(tmp_path, mock_utils_logger):
    """Test extraction directory normalization."""
    single_root = tmp_path / "single"
    single_root.mkdir()
    child = single_root / "child"
    child.mkdir()
    assert normalize_extract_dir(single_root) == child

    multiple_root = tmp_path / "multiple"
    multiple_root.mkdir()
    (multiple_root / "a").mkdir()
    (multiple_root / "b").mkdir()
    assert normalize_extract_dir(multiple_root) == multiple_root

    file_root = tmp_path / "file_root"
    file_root.mkdir()
    (file_root / "only.txt").write_text("x")
    assert normalize_extract_dir(file_root) == file_root
