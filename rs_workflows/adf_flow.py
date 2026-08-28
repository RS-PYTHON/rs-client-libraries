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
import re
import shutil
import subprocess  # nosec B404
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

from dateutil.parser import parse as parse_date
from prefect import flow, get_run_logger, task
from pystac import Asset, Item

from rs_common.prefect_utils import s3_upload_dir, s3_upload_file
from rs_workflows.aux_flow import aux_staging_task
from rs_workflows.catalog_flow import publish
from rs_workflows.flow_utils import (
    AdfProcessIn,
    AdfType,
    AuxiliaryProductMapping,
    AuxiliarySource,
    DprProcessedItemMetadata,
    FlowEnv,
    FlowGeneratedProduct,
)
from rs_workflows.payload_generator import (
    fetch_csv_from_endpoint,
    find_s3_output_bucket,
)
from rs_workflows.utils.utils import download_and_extract_assets_task

# Path to the conversion scripts
ADF_ECMWA_SCRIPT_PATH = Path(__file__).parent / "adf_conversion" / "S00__ADF_ECMWA.py"
ADF_ECMWF_SCRIPT_PATH = Path(__file__).parent / "adf_conversion" / "S00__ADF_ECMWF.py"
ADF_GETAS_SCRIPT_PATH = Path(__file__).parent / "adf_conversion" / "S00__ADF_GETAS.py"
ADF_WATER_SCRIPT_PATH = Path(__file__).parent / "adf_conversion" / "S00__ADF_WATER.py"

# Sentinel value indicating the stb_convert_products tool should be used
STB_CONVERT_PRODUCTS = "stb_convert_products"


class AdfConversionConfig(NamedTuple):
    """Configuration needed to run one ADF conversion type."""

    required_types: list[str]
    generated_prod_type: str
    script_path: Path | str


class S03OlAdfConfig(NamedTuple):
    """Configuration specific to one S03 OL ADF type."""

    required_type: str
    generated_prod_type: str


S03_OL_ADF_TYPE_CONFIG: dict[str, S03OlAdfConfig] = {
    AdfType.S03_ADF_OLCAL: S03OlAdfConfig(required_type="OL_1_CAL_AX", generated_prod_type="ADF_OLCAL"),
    AdfType.S03_ADF_OLEOP: S03OlAdfConfig(required_type="OL_1_EO__AX", generated_prod_type="ADF_OLEOP"),
    AdfType.S03_ADF_OLINS: S03OlAdfConfig(required_type="OL_1_INS_AX", generated_prod_type="ADF_OLINS"),
    AdfType.S03_ADF_OLLUT: S03OlAdfConfig(required_type="OL_1_CLUTAX", generated_prod_type="ADF_OLLUT"),
    AdfType.S03_ADF_OLPRG: S03OlAdfConfig(required_type="OL_1_PRG_AX", generated_prod_type="ADF_OLPRG"),
    AdfType.S03_ADF_OLRAC: S03OlAdfConfig(required_type="OL_1_RAC_AX", generated_prod_type="ADF_OLRAC"),
    AdfType.S03_ADF_OLSPC: S03OlAdfConfig(required_type="OL_1_SPC_AX", generated_prod_type="ADF_OLSPC"),
    AdfType.S03_ADF_OLPPP: S03OlAdfConfig(required_type="OL_2_PPP_AX", generated_prod_type="ADF_OLPPP"),
    AdfType.S03_ADF_OLPCP: S03OlAdfConfig(required_type="OL_2_PCP_AX", generated_prod_type="ADF_OLPCP"),
    AdfType.S03_ADF_OLCLP: S03OlAdfConfig(required_type="OL_2_CLP_AX", generated_prod_type="ADF_OLCLP"),
    AdfType.S03_ADF_OLWVP: S03OlAdfConfig(required_type="OL_2_WVP_AX", generated_prod_type="ADF_OLWVP"),
    AdfType.S03_ADF_OLVGP: S03OlAdfConfig(required_type="OL_2_VGP_AX", generated_prod_type="ADF_OLVGP"),
}


ADF_TYPE_CONFIG: dict[str, AdfConversionConfig] = {
    # We need AX___MA1_AX and AX___MA2_AX for S00__ADF_ECMWA.
    # AX___MA2_AX is not used anymore in S00__ADF_ECMWA.py. Add it back here if the script uses it again.
    AdfType.S00__ADF_ECMWA: AdfConversionConfig(
        required_types=["AX___MA1_AX"],
        generated_prod_type="ADF_ECMWA",
        script_path=ADF_ECMWA_SCRIPT_PATH,
    ),
    # We need AX___MF1_AX and AX___MF2_AX for S00__ADF_ECMWF.
    # AX___MF2_AX is not used anymore in S00__ADF_ECMWF.py. Add it back here if the script uses it again.
    AdfType.S00__ADF_ECMWF: AdfConversionConfig(
        required_types=["AX___MF1_AX"],
        generated_prod_type="ADF_ECMWF",
        script_path=ADF_ECMWF_SCRIPT_PATH,
    ),
    AdfType.S00__ADF_GETAS: AdfConversionConfig(
        required_types=["AX___DEM_AX"],
        generated_prod_type="ADF_GETAS",
        script_path=ADF_GETAS_SCRIPT_PATH,
    ),
    AdfType.S00__ADF_WATER: AdfConversionConfig(
        required_types=[
            "AX___LWM_AX",
            "AX___CLM_AX",
            "AX___TRM_AX",
            "AX___OOM_AX",
            "SR_2_MLM_AX",
            "SR_2_SURFAX",
        ],
        generated_prod_type="ADF_WATER",
        script_path=ADF_WATER_SCRIPT_PATH,
    ),
    **{
        adf_type: AdfConversionConfig(
            required_types=[s03_config.required_type],
            generated_prod_type=s03_config.generated_prod_type,
            script_path=STB_CONVERT_PRODUCTS,
        )
        for adf_type, s03_config in S03_OL_ADF_TYPE_CONFIG.items()
    },
}

STAC_DATETIME_PROPERTY_NAMES = {
    "created",
    "updated",
    "published",
    "datetime",
    "start_datetime",
    "end_datetime",
}

# Regex to capture start and end datetime tokens from an item_id.
# Expected pattern: <prefix>_<YYYYMMDDTHHMMSS>_<YYYYMMDDTHHMMSS>_<creation_timestamp>
_ITEM_ID_DT_RE = re.compile(r"_(\d{8}T\d{6})_(\d{8}T\d{6})_\d{8}T\d{6}$")


def extract_datetimes_from_item_id(item_id: str) -> tuple[str, str] | None:
    """Extract start and end datetime ISO strings from an item_id.

    The expected item_id format is:
        <prefix>_<start_YYYYMMDDTHHMMSS>_<end_YYYYMMDDTHHMMSS>_<creation_YYYYMMDDTHHMMSS>

    Returns a (start_datetime, end_datetime) tuple on success, or None when the
    pattern does not match.
    """
    match = _ITEM_ID_DT_RE.search(item_id)
    if not match:
        return None
    start_raw, end_raw = match.group(1), match.group(2)
    start_iso = (
        datetime.strptime(start_raw, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    )
    end_iso = (
        datetime.strptime(end_raw, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    )
    return start_iso, end_iso


def resolve_collection_name(mappings, product_type: str) -> str | None:
    """Resolve a collection name from mappings, preferring an exact match over '*'."""
    fallback_collection = None
    for mapping in mappings:
        if mapping.product_type == product_type:
            return mapping.collection_name
        if mapping.product_type == "*" and fallback_collection is None:
            fallback_collection = mapping.collection_name
    return fallback_collection


def resolve_source_name(mappings: list[AuxiliaryProductMapping], product_type: str) -> AuxiliarySource:
    """Resolve the source from mappings."""
    for mapping in mappings:
        if mapping.product_type == product_type:
            return mapping.source
    return AuxiliaryProductMapping.model_fields["source"].default  # pylint: disable=unsubscriptable-object


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


@task(name="Run ADF conversion")
def run_adf_script(script_path: Path | str, data_dir: Path, working_dir: Path, output_dir: Path) -> list[Path]:
    """
    Run an ADF conversion tool with the given input and output directories.

    When *script_path* is a Path, the corresponding Python script is executed
    with the current interpreter.  When it equals :data:`STB_CONVERT_PRODUCTS`,
    the ``stb_convert_products`` executable is called instead.

    Returns the list of generated ZARR product directories and JSON files.
    """
    logger = get_run_logger()
    logger.info(f"Running ADF conversion: {script_path}")

    def log_subprocess_output(output: str):
        """Forward subprocess output to the Prefect logger line by line."""
        for line in output.splitlines():
            logger.info(f"Conversion log: {line}")

    # Build command depending on the conversion tool
    if script_path == STB_CONVERT_PRODUCTS:
        command = ["stb_convert_products", "-i", str(data_dir), "-o", str(output_dir)]
        env = None  # inherit current environment
    else:
        env = os.environ.copy()
        env["ADF_OUTPUT"] = str(output_dir)
        # ADF_INPUT is used specifically by the script S00__ADF_GETAS.py to find the input data
        # we also pass it as an argument for the other scripts that don't use this variable, but
        # they receive it as an argument without causing any issue
        env["ADF_INPUT"] = str(data_dir)
        command = [sys.executable, str(script_path), str(data_dir), "--working_dir", str(working_dir)]

    try:
        result = subprocess.run(  # nosec B603
            # trusted local script / executable with flow-created paths
            command,
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
        logger.error(f"ADF conversion failed with exit code {e.returncode}")
        raise

    # find the generated ZARR directories and JSON files in output_dir
    zarr_products = sorted(output_dir.glob("*.zarr"))
    json_products = sorted(output_dir.glob("*.json"))
    all_products = zarr_products + json_products
    if not all_products:
        raise RuntimeError(
            f"No ZARR or JSON product generated in {output_dir}. The content of this dir is: "
            f"{list(output_dir.glob('*'))}",
        )

    return all_products


@task(name="Create STAC Item from ZARR metadata")
def create_stac_item_from_zarr(zarr_path: Path, generated_prod_type: str) -> Item:
    """
    Create a STAC Item from a generated ZARR directory or JSON file.

    For ZARR directories, metadata is read from .zattrs or zarr.json.
    For JSON files, the file itself is the metadata and the item ID is
    taken from stac_discovery.id when present, otherwise it is derived
    from the filename (minus the .json extension).
    """
    logger = get_run_logger()
    is_json_product = zarr_path.suffix == ".json"

    if is_json_product:
        logger.info(f"Creating STAC item from JSON file: {zarr_path}")
        metadata_path = zarr_path
    else:
        logger.info(f"Creating STAC item from ZARR: {zarr_path}")
        # read .zattrs for global attributes
        metadata_path = zarr_path / ".zattrs"
        if not metadata_path.exists():
            # try zarr.json if .zattrs doesn't exist
            metadata_path = zarr_path / "zarr.json"

    if not metadata_path.exists():
        raise RuntimeError(f"Metadata file (.zattrs or zarr.json) not found in {zarr_path}")

    with open(metadata_path, encoding="utf-8") as f:
        metadata = json.load(f)

    stac_discovery = metadata.get("stac_discovery") if isinstance(metadata.get("stac_discovery"), dict) else {}
    stac_metadata = stac_discovery if is_json_product and stac_discovery else metadata

    # extract STAC properties from metadata.
    # the scripts put them in 'properties' attribute.
    logger.info(f"Stac discovery metadata: {stac_discovery}")
    logger.info(f'Product metadata: {stac_metadata.get("properties", {})}')
    stac_props = normalize_stac_properties_datetimes(stac_metadata.get("properties", {}))
    logger.info(f"Extracted STAC properties from product metadata: {stac_props}")

    # requirement: "Create a STAC item ... with generated product type as product:type"
    # but the script may set it differently
    stac_props["product:type"] = generated_prod_type

    # For JSON files prefer the STAC discovery item ID, otherwise use the filename
    # without the .json extension. For ZARR directories fall back to metadata ID.
    if is_json_product:
        item_id = stac_discovery.get("id") or zarr_path.stem
    else:
        item_id = metadata.get("id", zarr_path.stem)
    logger.info(f"Setting item_id to {item_id}")

    # extract start/end datetime for pystac.Item validation.
    # try to find them in the metadata, but if they are not present, try to take them from the
    # item_id string pattern. If still not found, raise an error.
    # priority: metadata properties > datetimes embedded in item_id
    start_dt_str = stac_props.get("start_datetime")
    end_dt_str = stac_props.get("end_datetime")
    if not start_dt_str or not end_dt_str:
        id_datetimes = extract_datetimes_from_item_id(item_id)
        if id_datetimes:
            start_dt_str = start_dt_str or id_datetimes[0]
            end_dt_str = end_dt_str or id_datetimes[1]
            logger.info(f"Extracted start/end datetimes from item_id: {id_datetimes[0]} / {id_datetimes[1]}")
        else:
            msg = f"Could not extract datetimes from metadata properties or from item_id {item_id!r}"
            logger.error(msg)
            raise RuntimeError(msg)

    # We check if platform is part of the STAC properties.
    # If not, we will get it from the name of the generated item
    if not stac_props.get("platform"):
        value_platform: str = item_id[2:4].lower()
        if value_platform not in ("0_", "__", "00"):
            stac_props["platform"] = f"sentinel-{value_platform}"
            logger.info(
                "'platform' property has been computed from the item name." f"Value is '{stac_props["platform"]}'",
            )
        else:
            logger.info("'platform' is not added to properties because ADF is not specific to a platform.")

    start_dt = parse_date(start_dt_str) if start_dt_str else None
    end_dt = parse_date(end_dt_str) if end_dt_str else None

    # build basic STAC item
    item = Item(
        id=item_id,
        geometry=stac_metadata.get("geometry", None),
        bbox=stac_metadata.get("bbox", None),
        datetime=None,
        start_datetime=start_dt,
        end_datetime=end_dt,
        properties=stac_props,
    )

    # add product as an asset
    logger.info(f"Adding asset to STAC item with href {str(zarr_path)}")
    if is_json_product:
        item.add_asset(
            key="data",
            asset=Asset(
                href=str(zarr_path),
                title=item_id,
                media_type="application/json",
                roles=["data", "metadata"],
            ),
        )
    else:
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


class SafeDict(dict):
    """Dict subclass that returns {key} if key is missing or its value is None."""

    def __missing__(self, key):
        return "{" + key + "}"

    def __getitem__(self, key):
        value = super().__getitem__(key)
        if value is None:
            return "{" + key + "}"
        return value


def substitute_values(obj, values):
    """Recursively substitute values in a nested structure of dicts/lists/strings."""
    if isinstance(obj, dict):
        return {k: substitute_values(v, values) for k, v in obj.items()}
    if isinstance(obj, list):
        return [substitute_values(v, values) for v in obj]
    if isinstance(obj, str):
        return obj.format_map(SafeDict(values))
    return obj


@flow(name="convert-adf")
async def adf_conversion(adf_input: AdfProcessIn):
    """
    Prefect flow for ADF conversion.
    """
    logger = get_run_logger()
    logger.info(f"Starting adf_conversion flow for adf_type: {adf_input.adf_type}")

    flow_env = FlowEnv(adf_input.env)
    with flow_env.start_span(__name__, "adf_conversion"):
        # 1. Build CQL2 filters for required auxiliary types
        adf_config = ADF_TYPE_CONFIG.get(adf_input.adf_type)
        if adf_config is None:
            logger.error(f"Unsupported adf_type: {adf_input.adf_type}")
            return

        required_types = adf_config.required_types
        generated_prod_type = adf_config.generated_prod_type
        script_path = adf_config.script_path

        staged_items: list[Item] = []
        for prod_type in required_types:
            # find target collection from mapping, preferring an exact product type match
            target_collection = resolve_collection_name(
                adf_input.auxiliary_product_to_collection_identifier,
                prod_type,
            )
            if target_collection is None:
                raise RuntimeError(
                    "❌ No target collection found for input product type "
                    f"{prod_type!r} in auxiliary_product_to_collection_identifier.",
                )

            # find source from mapping
            source: AuxiliarySource = resolve_source_name(
                adf_input.auxiliary_product_to_collection_identifier,
                prod_type,
            )

            cql2_filter: dict = {}
            if adf_input.cql2_filter is not None:
                logger.info("🧹 Use the provided CQL2 filter for auxiliary data retrieval")
                cql2_filter = substitute_values(adf_input.cql2_filter, {"product_type": prod_type})
                logger.debug(f"Substituted CQL2 filter for product type {prod_type}: {cql2_filter}")
            else:
                logger.info("🧹 Build a default CQL2 filter for auxiliary data retrieval with operator t_intersects")
                cql2_filter = {
                    "filter": {
                        "op": "and",
                        "args": [
                            {
                                "op": "t_intersects",
                                "args": [
                                    {"interval": [{"property": "start_datetime"}, {"property": "end_datetime"}]},
                                    {
                                        "interval": [
                                            adf_input.start_datetime.isoformat() if adf_input.start_datetime else None,
                                            adf_input.end_datetime.isoformat() if adf_input.end_datetime else None,
                                        ],
                                    },
                                ],
                            },
                            {"op": "=", "args": [{"property": "product:type"}, prod_type]},
                        ],
                    },
                }

            # When the source is the catalog, no staging occurs: collection_name is both the source
            # and destination collection, so the search must be restricted to it, otherwise the STAC
            # search returns the most recent matching item across all collections (e.g. another OLCI
            # baseline collection). For other sources, collection_name is only the destination
            # collection where staged items will be uploaded, and must not be used to filter the
            # search performed against the external source.
            if source == AuxiliarySource.CATALOG:
                cql2_filter["collections"] = [target_collection]

            logger.info(f"Built CQL2 filter for product type {prod_type}: {cql2_filter}")

            logger.info(f"📥 Staging {prod_type} to collection {target_collection} from source {source}")
            success, items = await aux_staging_task(
                env=adf_input.env,
                cql2_filter=cql2_filter,
                catalog_collection_identifier=target_collection,
                source=source,
            )
            logger.debug(f"Staging result for product type {prod_type}: success={success}, items={items}")
            if success and items:
                staged_items.extend(items)

        if not staged_items:
            logger.warning("⚠️ No staged items found to process.")
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
            await download_and_extract_assets_task(staged_items, input_dir)  # type: ignore[arg-type]

            # 3. Call the conversion tool
            try:
                zarr_product_paths = run_adf_script(script_path, input_dir, work_dir, output_dir)
            finally:
                logger.info(f"Cleaning up input directory {input_dir}")
                shutil.rmtree(input_dir, ignore_errors=True)

            # 4. Process each generated ZARR product
            # compute bucket configuration once for all products
            bucket_configuration = fetch_csv_from_endpoint(os.environ["RSPY_HOST_OSAM"] + "/internal/configuration")
            owner_id = flow_env.owner_id

            items_metadata: list[DprProcessedItemMetadata] = []
            publish_mapping: list[FlowGeneratedProduct] = []

            for zarr_product_path in zarr_product_paths:
                # 5. Create STAC item for this ZARR
                stac_item = create_stac_item_from_zarr(zarr_product_path, generated_prod_type)
                stac_item.properties["product:type"] = generated_prod_type

                # 6. Resolve destination collection
                target_collection = resolve_collection_name(
                    adf_input.auxiliary_product_to_collection_identifier,
                    generated_prod_type,
                )
                if target_collection is None:
                    raise RuntimeError(
                        "❌ No target collection found for generated product type "
                        f"{generated_prod_type!r} in auxiliary_product_to_collection_identifier.",
                    )

                bucket_name = find_s3_output_bucket(
                    bucket_configuration,
                    owner_id,
                    target_collection,
                    generated_prod_type,
                )

                # 7. Upload product to S3 and update STAC item href
                if zarr_product_path.suffix == ".json":
                    s3_dest = f"s3://{bucket_name}/{owner_id}/{target_collection}/{zarr_product_path.name}"
                    logger.info(f"Uploading JSON to {s3_dest}")
                    await s3_upload_file(zarr_product_path, s3_dest)
                    stac_item.assets["data"].href = s3_dest
                else:
                    zarr_suffix = ".zarr" if zarr_product_path.suffix == ".zarr" else ""
                    s3_dest_prefix = f"s3://{bucket_name}/{owner_id}/{target_collection}/{stac_item.id}{zarr_suffix}/"
                    logger.info(f"Uploading ZARR to {s3_dest_prefix}")
                    await s3_upload_dir(zarr_product_path, s3_dest_prefix)
                    stac_item.assets["data"].href = s3_dest_prefix

                items_metadata.append(
                    DprProcessedItemMetadata(
                        output_product_id=stac_item.id,
                        product_type=generated_prod_type,
                        stac_item=stac_item,
                    ),
                )
                publish_mapping.append(
                    FlowGeneratedProduct(
                        name=stac_item.id,
                        product_type=generated_prod_type,
                        collection_name=target_collection,
                    ),
                )

            # 8. Publish all items to catalog
            try:
                await publish(
                    adf_input.env,
                    publish_mapping,
                    items_metadata,
                )
            finally:
                logger.info(f"Cleaning up output directory {output_dir}")
                shutil.rmtree(output_dir, ignore_errors=True)


###########################
# Call the flows as tasks #
###########################
@task(name="convert-adf-task")
async def adf_conversion_task(*args, **kwargs) -> None:
    """See: adf_conversion"""
    return await adf_conversion.fn(*args, **kwargs)


if __name__ == "__main__":
    # For testing purposes
    pass
