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

"""sentinel 1 ARD processing."""

import re
from datetime import date, datetime
from typing import Any, Self

from prefect import flow, get_run_logger, task
from pydantic import Field
from pystac import Item, ItemCollection

from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
    FlowInputProduct,
)
from rs_workflows.on_demand.common.types import ProcessingFlowParams
from rs_workflows.utils.catalog import get_catalog_items
from rs_workflows.utils.dpr import call_dpr_flow


class Level1ArdFlowParams(ProcessingFlowParams):
    """
    Parameters to override default Prefect variable 'sx-l1ard-default-setting'..

    There is no need to set all of them.
    Only the ones you want to override from default settings.
    optional type not used.
    """

    input_collections: list[str] = Field(
        default=["s01sewslc", "s01siwslc", "s01ssmslc"],
        title="Input Collections",
        description="List of Catalog collection names containing the Input products.",
        json_schema_extra={"order": 4},
    )

    def _resolve_specific(self, settings: dict[str, Any]) -> dict[str, Any]:
        return {
            "input_collections": self.input_collections or settings.get("input_collections", ""),
        }

    async def resolve(self, mission: str) -> Self:
        """
        Merge data from Prefect variable and parameters called.
        """
        return await super()._resolve(mission, "1ard")


# Example to retrieve April 2026 S1 SLC (IW) products from CDSE in Toulouse area:
# https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-slc/items?datetime=2026-04-01T00%3A00%3A00.000Z%2F2026-04-30T00%3A00%3A00.000Z&bbox=1.49319102647564%2C43.55965105873602%2C1.5043201542126%2C43.5629914174788&limit=6&sortby=-properties.created

# To retrieve bursts:
# https://catalogue.dataspace.copernicus.eu/odata/v1/Bursts?$top=50&$filter=ParentProductName%20eq%20%27S1C_IW_SLC__1SDV_20260408T174632_20260408T174659_007128_00E6F6_E998.SAFE%27

ALLOWED_INSTRUMENT_MODES = {"IW", "EW", "SM"}

# Shared folder used by the S1-ARD processor to write its temporary files
TEMPORARY_FOLDER = "/mnt/share/ads-01"


def _get_slc_collection(slc: str, input_collections: list[str]) -> str:
    """
    Return the catalog collection of an S1 SLC product, based on the instrument mode in its name.

    Args:
        slc (str): STAC item identifier of the S1 SLC product, e.g. S1C_IW_SLC__1SDV_...
        input_collections (list[str]): Catalog collection names containing the S1 SLC products,
        e.g. s01sewslc, s01siwslc, s01ssmslc.

    Returns:
        str: The collection matching the instrument mode (IW, EW, or SM for S1..S6 stripmap modes),
        or the first collection if no match is found.
    """
    match = re.match(r"^S1[A-Z]_(IW|EW|S[1-6])_", slc)
    if match:
        mode = "sm" if match.group(1).startswith("S") else match.group(1).lower()
        collection = next((c for c in input_collections if c.lower().endswith(f"{mode}slc")), None)
        if collection:
            return collection
    return input_collections[0]


def _check_slcs(slcs: list[str], reference_date: date | None) -> bool:
    """Check the input SLCs and reference date. Return False if the processing cannot be launched."""
    logger = get_run_logger()
    num_slcs = len(slcs)
    if num_slcs == 0:
        logger.error("❌ The processing cannot be launched, no SLC provided")
        return False
    if not reference_date and num_slcs > 1:
        logger.error("❌ The processing cannot be launched, reference_date is required when providing several slcs")
        return False
    if reference_date and num_slcs == 1:
        logger.warning("⚠️ reference_date is not required when providing a single slc")
    return True


def _get_instrument_mode(items: list[Item]) -> str | None:
    """Return the instrument mode shared by all the items, or None if they are invalid or different."""
    logger = get_run_logger()
    modes: list[str] = [item.properties.get("sar:instrument_mode", "") for item in items]
    invalid_modes = [mode for mode in modes if mode not in ALLOWED_INSTRUMENT_MODES]
    if invalid_modes:
        logger.error(
            f"❌ Invalid sar:instrument_mode values found: {invalid_modes}. "
            f"Allowed values are {sorted(ALLOWED_INSTRUMENT_MODES)}",
        )
        return None

    unique_modes = set(modes)
    if len(unique_modes) != 1:
        logger.error(f"❌ Items do not share the same sar:instrument_mode: {sorted(unique_modes)}")
        return None
    return unique_modes.pop()


def _build_external_variables(
    instrument_mode: str,
    slcs: list[str],
    reference_date: date | None,
    edh_api_key: str,
    pipeline: str | None,
) -> dict[str, str]:
    """Build the external variables passed to the DPR processor."""
    external_variables = {"instrument_mode": instrument_mode}
    if pipeline == "SLC__1_NRB__1" and not reference_date:
        # Reference date is required to fetch S2_TILES aux file (mosaicking step only)
        match = re.search(r"(\d{8})T", slcs[0])
        if match:
            reference_date = datetime.strptime(match.group(1), "%Y%m%d").date()
    if reference_date:
        external_variables["reference_date"] = str(reference_date)
    if edh_api_key:
        external_variables["edh_api_key"] = edh_api_key
    return external_variables


async def _do_process_s1ard(
    slcs: list[str],
    reference_date: date | None,
    edh_api_key: str,
    flow_params: Level1ArdFlowParams,
    verbose: bool = False,  # pylint: disable=unused-argument
):
    """
    Sentinel-1 ARD processing (common function for reference pipeline and nominal on-demand flow).

    Args:
        slcs (list[str]): STAC item identifiers of S1 SLC input products, already converted to zarr.
        A single item for the reference pipeline.
        reference_date (date | None): Date of the master (reference) SLC. Set to None for reference pipeline
        edh_api_key: Destination Earth / EarthDataHub standard API key used to access Copernicus DEM
        flow_params (Level1ArdFlowParams): Flow parameters
        verbose (bool, optional): not used yet. Defaults to False.
    """
    logger = get_run_logger()

    if not _check_slcs(slcs, reference_date):
        return

    # Resolve parameters
    mission = "1"
    p = await flow_params.resolve(mission)
    flow_env = FlowEnv(FlowEnvArgs(owner_id=p.owner_identifier))
    suffix = "-reference" if not reference_date else ""
    # start_span is a context manager (decorated with _agnosticcontextmanager), not detected by sonar
    with flow_env.start_span(__name__, f"sentinel{mission}-level1ard-processing{suffix}"):  # NOSONAR
        item_collection: ItemCollection | None = await get_catalog_items(flow_env, slcs, p.input_collections)
        if not item_collection or len(item_collection) == 0:
            logger.error(f"❌ The processing cannot be launched, no items found for {slcs} in {p.input_collections}")
            return

        instrument_mode = _get_instrument_mode(item_collection.items)
        if not instrument_mode:
            return

        input_products = [
            FlowInputProduct(
                name="slcs",
                item_id=slc,
                collection_name=_get_slc_collection(slc, p.input_collections),
            )
            for slc in slcs
        ]

        # Call DPR flow
        await call_dpr_flow(
            FlowEnvArgs(owner_id=p.owner_identifier),
            input_products=input_products,
            external_variables=_build_external_variables(
                instrument_mode,
                slcs,
                reference_date,
                edh_api_key,
                p.pipeline,
            ),
            dask_cluster_label=p.dask_cluster_label,
            processor_name=p.processor_name,
            processor_version=p.processor_version,
            pipeline=p.pipeline,
            unit=p.unit,
            priority=p.priority,
            processing_mode=p.processing_mode,
            workflow=p.workflow,
            generated_product_to_collection_identifier=p.generated_product_to_collection_identifier or [],
            auxiliary_product_to_collection_identifier=p.auxiliary_product_to_collection_identifier or [],
            dask_task_timeout=3000,
            temporary_folder=TEMPORARY_FOLDER,
            temporary_shared=True,
        )


@flow(name="process-s1-ard")
async def process_s1ard(
    slcs: list[str],
    reference_date: date,
    edh_api_key: str,
    flow_params: Level1ArdFlowParams,
    verbose: bool = False,
):
    """
    Sentinel-1 ARD processing (nominal on-demand flow).

        Args:
            slcs: STAC item identifiers of S1 SLC input products, already converted to zarr.
            reference_date: Date of the master (reference) SLC
            edh_api_key: Destination Earth / EarthDataHub standard API key used to access Copernicus DEM
    """

    # Specific data used for tests:
    # https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-slc/items/S1D_IW_SLC__1SDV_20260420T055953_20260420T060020_002426_003FB6_3AE6
    # https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-slc/items/S1C_IW_SLC__1SDV_20260408T174632_20260408T174659_007128_00E6F6_E998
    # https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-slc/items/S1C_IW_SLC__1SDV_20260327T174632_20260327T174659_006953_00E115_D167

    # https://earthdatahub.destine.eu/api/stac/v1/collections/copernicus-dem/items/GLO-30

    await _do_process_s1ard(slcs, reference_date, edh_api_key, flow_params, verbose)


@flow(name="process-s1-ard-reference")
async def process_s1ard_reference(slc: str, edh_api_key: str, flow_params: Level1ArdFlowParams, verbose: bool = False):
    """
    Sentinel-1 ARD processing reference pipeline.
    This flow must be run once to generate the REFERENCE_DB used in on-demand S1 ARD processing flow.

        Args:
            slc: STAC item identifier of S1 master SLC input product, already converted to zarr.
            edh_api_key: Destination Earth / EarthDataHub standard API key used to access Copernicus DEM
    """
    await _do_process_s1ard([slc], None, edh_api_key, flow_params, verbose)


@task(name="process sentinel-1 level-1-ard")
async def process_s1l1ard_task(*args, **kwargs) -> None:
    """See: dpr_processing"""
    return await process_s1ard.fn(*args, **kwargs)


@task(name="process sentinel-1 level-1-ard reference pipeline")
async def process_s1l1ard_reference_task(*args, **kwargs) -> None:
    """See: dpr_processing"""
    return await process_s1ard_reference.fn(*args, **kwargs)
