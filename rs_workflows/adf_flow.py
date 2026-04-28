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

"""Adf conversion flow implementation."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import timezone
from pathlib import Path

from dateutil.parser import parse as parse_date
from prefect import flow, get_run_logger, task
from pystac import Asset, Item

from rs_common.prefect_utils import s3_download_file, s3_upload_dir
from rs_common.utils import extract_tar, extract_zip, recursive_extract
from rs_workflows.auxip_flow import auxip_staging_task
from rs_workflows.catalog_flow import publish
from rs_workflows.flow_utils import (
    AdfProcessIn,
    AdfType,
    DprProcessedItemMetadata,
    FlowEnv,
    FlowGeneratedProduct,
)
from rs_workflows.payload_generator import (
    fetch_csv_from_endpoint,
    find_s3_output_bucket,
)

# Path to the S00__ADF_ECMWA script
ADF_ECMWA_SCRIPT_PATH = Path(__file__).parent / "adf_conversion" / "S00__ADF_ECMWA.py"

STAC_DATETIME_PROPERTY_NAMES = {
    "created",
    "updated",
    "published",
    "datetime",
    "start_datetime",
    "end_datetime",
}


def normalize_stac_datetime_value(value: str) -> str:
    """Normalize a datetime string to an RFC 3339 UTC representation."""
    parsed_value = parse_date(value)
    if parsed_value.tzinfo is None:
        parsed_value = parsed_value.replace(tzinfo=timezone.utc)
    else:
        parsed_value = parsed_value.astimezone(timezone.utc)
    return parsed_value.isoformat().replace("+00:00", "Z")


def normalize_stac_properties_datetimes(stac_props: dict) -> dict:
    """Normalize STAC datetime-like property values to include an explicit timezone."""
    normalized_props = dict(stac_props)
    for key in STAC_DATETIME_PROPERTY_NAMES:
        value = normalized_props.get(key)
        if isinstance(value, str):
            normalized_props[key] = normalize_stac_datetime_value(value)
    return normalized_props


@task(name="download_and_extract_assets")
async def download_and_extract_assets_task(items: list[Item], extract_to: Path):
    """
    Download and extract all assets from the given items to the destination directory.

    Args:
        items: List of STAC items containing assets to download.
        extract_to: Local directory where assets should be extracted.
    """
    logger = get_run_logger()

    for item in items:
        for asset_name, asset in item.assets.items():
            if not asset.href.startswith("s3://"):
                logger.warning(f"Skipping non-S3 asset: {asset_name} ({asset.href})")
                continue

            # download to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(asset.href).suffix) as tmp_file:
                tmp_path = Path(tmp_file.name)

            try:
                logger.info(f"Downloading asset {asset_name} from {asset.href}")
                await s3_download_file(asset.href, tmp_path)

                # extract or move to destination
                if asset.href.lower().endswith((".zip", ".tar", ".tgz", ".tar.gz")):
                    logger.info(f"Extracting {asset.href} to {extract_to}")
                    if asset.href.lower().endswith(".zip"):
                        extract_zip(tmp_path, extract_to)
                    else:
                        extract_tar(tmp_path, extract_to)

                    # handle nested archives (common in AUXIP)
                    recursive_extract(extract_to)
                else:
                    # not an archive, just copy/move to destination
                    dest_path = extract_to / Path(asset.href).name
                    logger.info(f"Copying {asset.href} to {dest_path}")
                    shutil.copy(tmp_path, dest_path)
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()


@task(name="Run S00__ADF_ECMWA conversion script")
def run_adf_ecmwa_script(data_dir: Path, working_dir: Path, output_dir: Path) -> Path:
    """
    Run the S00__ADF_ECMWA.py script with the given input and output directories.
    Returns the path to the generated ZARR product.
    """
    logger = get_run_logger()
    logger.info(f"Running ADF conversion script: {ADF_ECMWA_SCRIPT_PATH}")

    env = os.environ.copy()
    env["ADF_OUTPUT"] = str(output_dir)

    def log_subprocess_output(output: str):
        """Forward subprocess output to the Prefect logger line by line."""
        for line in output.splitlines():
            logger.info(f"Conversion script log: {line}")

    # the script expects data_dir as first argument
    try:
        result = subprocess.run(
            [sys.executable, str(ADF_ECMWA_SCRIPT_PATH), str(data_dir), "--working_dir", str(working_dir)],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        log_subprocess_output(result.stdout)
        log_subprocess_output(result.stderr)
    except subprocess.CalledProcessError as e:
        log_subprocess_output(e.stdout or "")
        log_subprocess_output(e.stderr or "")
        logger.error(f"ADF conversion script failed with exit code {e.returncode}")
        raise

    # find the generated ZARR directory in output_dir
    zarr_products = list(output_dir.glob("*.zarr"))
    if not zarr_products:
        raise RuntimeError(
            f"No ZARR product generated in {output_dir}. The content of this dir is: " f"{list(output_dir.glob('*'))}",
        )

    # should be only one
    return zarr_products[0]


@task(name="Create STAC Item from ZARR metadata")
def create_stac_item_from_zarr(zarr_path: Path) -> Item:
    """
    Create a STAC Item from the .zattrs or zarr.json in the generated ZARR tree.
    """
    logger = get_run_logger()
    logger.info(f"Creating STAC item from ZARR: {zarr_path}")

    # read .zattrs for global attributes
    zattrs_path = zarr_path / ".zattrs"
    if not zattrs_path.exists():
        # try zarr.json if .zattrs doesn't exist (though xarray usually creates .zattrs)
        zattrs_path = zarr_path / "zarr.json"

    if not zattrs_path.exists():
        raise RuntimeError(f"Metadata file (.zattrs or zarr.json) not found in {zarr_path}")

    with open(zattrs_path, encoding="utf-8") as f:
        metadata = json.load(f)

    # extract STAC properties from metadata.
    # the script S00__ADF_ECMWA.py puts them in 'properties' attribute.
    logger.info(f"ZARR metadata: {metadata.get("properties", {})}")
    stac_props = normalize_stac_properties_datetimes(metadata.get("properties", {}))
    logger.info(f"Extracted STAC properties from ZARR metadata: {stac_props}")

    # requirement: "Create a STAC item ... with S00__ADF_ECMWA as product:type"
    # but the script sets it to "ADF_ECMWA"
    stac_props["product:type"] = "S00__ADF_ECMWA"

    item_id = metadata.get("id", zarr_path.stem)
    logger.info(f"Setting item_id to {item_id}")

    # extract start/end datetime for pystac.Item validation
    start_dt_str = stac_props.get("start_datetime")
    end_dt_str = stac_props.get("end_datetime")

    start_dt = parse_date(start_dt_str) if start_dt_str else None
    end_dt = parse_date(end_dt_str) if end_dt_str else None

    # build basic STAC item
    item = Item(
        id=item_id,
        geometry=None,
        bbox=None,
        datetime=None,
        start_datetime=start_dt,
        end_datetime=end_dt,
        properties=stac_props,
    )

    # add ZARR folder as an asset
    item.add_asset(
        key="data",
        asset=Asset(
            href=str(zarr_path),
            title=item_id,
            media_type="application/vnd+zarr",
            roles=["data", "metadata"],
        ),
    )

    return item


@flow(name="adf_conversion")
async def adf_conversion(adf_input: AdfProcessIn):
    """
    Prefect flow for ADF conversion.
    """
    logger = get_run_logger()
    logger.info(f"Starting adf_conversion flow for adf_type: {adf_input.adf_type}")

    flow_env = FlowEnv(adf_input.env)
    with flow_env.start_span(__name__, "adf_conversion"):

        if adf_input.adf_type == AdfType.S00__ADF_ECMWA:
            # 1. Build CQL2 filters for required auxiliary types
            # We need AX___MA1_AX and AX___MA2_AX for S00__ADF_ECMWA
            # required_types = ["AX___MA1_AX", "AX___MA2_AX"]
            # AX___MA2_AX is not used anymore in S00__ADF_ECMWA.py. uncomment the prev line and remove the next one
            # if a change in the script is made to use it again
            required_types = ["AX___MA1_AX"]

            staged_items: list[Item] = []
            for prod_type in required_types:
                cql2_filter = {
                    "filter": {
                        "op": "and",
                        "args": [
                            {
                                "op": "t_intersects",
                                "args": [
                                    {"property": "datetime"},
                                    [
                                        adf_input.start_datetime.isoformat() if adf_input.start_datetime else None,
                                        adf_input.end_datetime.isoformat() if adf_input.end_datetime else None,
                                    ],
                                ],
                            },
                            {"op": "=", "args": [{"property": "product:type"}, prod_type]},
                        ],
                    },
                }
                logger.info(f"Built CQL2 filter for product type {prod_type}: {cql2_filter}")
                # find target collection from mapping
                target_collection = "AUX"
                for mapping in adf_input.auxiliary_product_to_collection_identifier:
                    if mapping.product_type in (prod_type, "*"):
                        target_collection = mapping.collection_name or prod_type
                        break

                logger.info(f"Staging {prod_type} to collection {target_collection}")
                success, items = await auxip_staging_task(
                    env=adf_input.env,
                    cql2_filter=cql2_filter,
                    catalog_collection_identifier=target_collection,
                )
                if success and items:
                    staged_items.extend(items)

            if not staged_items:
                logger.error("No staged items found to process.")
                return

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                input_dir = temp_path / "INPUT"
                work_dir = temp_path / "WORK"
                output_dir = temp_path / "OUTPUT"

                input_dir.mkdir()
                work_dir.mkdir()
                output_dir.mkdir()

                # 2. Download and unzip assets locally
                await download_and_extract_assets_task(staged_items, input_dir)

                # 3. Call the S00__ADF_ECMWA.py script
                try:
                    zarr_product_path = run_adf_ecmwa_script(input_dir, work_dir, output_dir)
                finally:
                    shutil.rmtree(input_dir, ignore_errors=True)

                # 4. Create STAC item for the generated ZARR
                stac_item = create_stac_item_from_zarr(zarr_product_path)

                # 5. Copy ZARR to catalog bucket
                # compute location using find_s3_output_bucket
                bucket_configuration = fetch_csv_from_endpoint(os.environ["RSPY_HOST_OSAM"] + "/internal/configuration")

                # find destination collection and product type for the generated ADF
                # the script says product:type is ADF_ECMWA but we workaround it to S00__ADF_ECMWA
                generated_prod_type = "S00__ADF_ECMWA"
                owner_id = flow_env.owner_id

                # resolve target collection for publishing
                # the user mapping should contain the entry for this new product type
                publish_collection = None
                wildcard_collection = None
                for mapping in adf_input.auxiliary_product_to_collection_identifier:
                    if mapping.product_type == generated_prod_type:
                        publish_collection = mapping.collection_name or generated_prod_type
                        break
                    if mapping.product_type == "*" and wildcard_collection is None:
                        wildcard_collection = mapping.collection_name or generated_prod_type
                else:
                    if wildcard_collection is not None:
                        publish_collection = wildcard_collection

                if publish_collection is None:
                    raise RuntimeError(
                        "No publish collection found for generated product type "
                        f"{generated_prod_type!r} in auxiliary_product_to_collection_identifier.",
                    )

                bucket_name = find_s3_output_bucket(
                    bucket_configuration,
                    owner_id,
                    publish_collection,
                    generated_prod_type,
                )

                s3_dest_prefix = f"s3://{bucket_name}/{owner_id}/{publish_collection}/{stac_item.id}/"
                logger.info(f"Uploading ZARR to {s3_dest_prefix}")

                # upload folder
                await s3_upload_dir(zarr_product_path, s3_dest_prefix)

                # update STAC item href to point to S3
                stac_item.assets["data"].href = s3_dest_prefix

                # 8. Publish to catalog
                # items_metadata: list[DprProcessedItemMetadata]
                items_metadata = [
                    DprProcessedItemMetadata(
                        output_product_id=stac_item.id,
                        product_type=generated_prod_type,
                        stac_item=stac_item,
                    ),
                ]

                # generated_product_to_collection_identifier: list[FlowGeneratedProduct]
                # we reuse the mapping from AdfProcessIn
                publish_mapping = [
                    FlowGeneratedProduct(
                        name=stac_item.id,
                        product_type=generated_prod_type,
                        collection_name=publish_collection,
                    ),
                ]

                try:
                    await publish(
                        adf_input.env,
                        publish_mapping,
                        items_metadata,
                    )
                finally:
                    shutil.rmtree(output_dir, ignore_errors=True)

        else:
            logger.error(f"Unsupported adf_type: {adf_input.adf_type}")


if __name__ == "__main__":
    # For testing purposes
    pass
