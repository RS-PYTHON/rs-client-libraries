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

"""Helper task to interact with the DPR as a service."""

import json
import time
from enum import Enum
from pathlib import Path
from typing import Any

import anyio
from jsonschema import ValidationError, validate
from prefect import task
from prefect.variables import Variable
from pydantic import BaseModel, Field

from rs_client.ogcapi.dpr_client import (
    DprPipeline,
    DprProcessor,
)
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessIn,
    FlowEnvArgs,
    FlowGeneratedProduct,
    FlowInputProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_workflows.on_demand.common.events import emit_product_generated_events
from rs_workflows.on_demand_processing import dpr_processing
from rs_workflows.utils.dask import is_dask_cluster_running
from rs_workflows.utils.prefect import get_logger

DASK_CLUSTER_STAGING = "dask-staging"
PROCESSOR_SETTINGS_SCHEMA = Path(__file__).parents[2] / "config" / "ProcessorDefaultSettings.schema.json"
PREFECT_VAR_NAME = "processing-storage-configuration"
# TODO: Once the namespace issue is resolved, we can remove this hard coded path and
# use the shared disk path instead implemented in generate_payload_path().
KUBERENETES_COMMON_NAMESPACE_FOR_DASK_AND_PREFECT = True


class ProcessingId(str, Enum):
    """Identifier of a processing, that gives the name of its `<processing_id>-default-setting` Prefect variable."""

    S1_L0 = "s1l0"
    S1_ARD = "s1ard"
    S3_L0 = "s3l0"
    S3_OLCI_L1 = "s3l1olci"
    S3_OLCI_L2 = "s3l2olci"


class ProcessorInfo(BaseModel):
    """Processor run by a processing."""

    name: str = Field(description="Name of the processor, as a process of rs-dpr-service.")
    version: str = Field(description="Version of the processor.")


class ProcessorSettings(BaseModel):
    """
    Default settings of a processing, read from its `<processing_id>-default-setting` Prefect variable, that must
    match the `config/ProcessorDefaultSettings.schema.json` JSON schema.
    """

    processor: ProcessorInfo
    pipeline: str | None = Field(default=None, description="Pipeline of the task table to run.")
    unit: str | None = Field(default=None, description="Unit of the task table to run, instead of a pipeline.")
    priority: Priority
    processing_mode: list[ProcessingMode]
    dask_cluster_name: str = Field(description="Label of the Dask cluster where the processor runs.")
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping]
    generated_product_to_collection_identifier: list[FlowGeneratedProduct]
    input_collections: list[str] = Field(default=[], description="Catalog collections of the input products.")
    cadip_collections: list[str] = Field(default=[], description="Level-0: CADIP collections to stage the sessions.")


async def load_processor_settings(processing_id: ProcessingId) -> ProcessorSettings:
    """
    Read the default settings of a processing from its `<processing_id>-default-setting` Prefect variable, check them
    against their JSON schema, and check that the Dask clusters of the processing are running.

    The flows call it first, before searching their inputs in the catalog or the external stations, so that a
    configuration error or an unavailable Dask cluster is reported as soon as possible.

    Args:
        processing_id: Identifier of the processing.

    Returns:
        The default settings of the processing.

    Raises:
        RuntimeError: If the Prefect variable is missing.
        ValueError: If the Prefect variable doesn't match the JSON schema, or if a Dask cluster is not running.
    """
    variable_name = f"{processing_id.value}-default-setting"
    get_logger().info(f"Read Prefect variable {variable_name!r}")
    raw_settings = await Variable.get(variable_name)  # type: ignore[misc]
    if raw_settings is None:
        raise RuntimeError(f"❌ Prefect variable {variable_name!r} is missing")

    async with await anyio.open_file(PROCESSOR_SETTINGS_SCHEMA, encoding="utf-8") as f:
        schema = json.loads(await f.read())
    try:
        validate(instance=raw_settings, schema=schema)
    except ValidationError as err:
        raise ValueError(
            f"❌ Invalid JSON schema for var set in Prefect variable {variable_name!r}:\n{err.message}",
        ) from err
    settings = ProcessorSettings.model_validate(raw_settings)

    # The staging Dask cluster stages the auxiliary files of the processor
    for dask_cluster in (settings.dask_cluster_name, DASK_CLUSTER_STAGING):
        if not await is_dask_cluster_running(dask_cluster):
            raise ValueError(f"❌ '{dask_cluster}' is unknown or not ready.")
    return settings


async def generate_payload_path(owner_id: str) -> str:
    """
    Generates the shared disk path used to store a processing payload for a processor

    This function retrieves the storage configuration from a Prefect variable
    and searches for the first shared-disk configuration that has a valid
    name and absolute path and uses the `CREATE_OVERWRITE` opening mode. The
    resulting shared-disk path is combined with the owner identifier and the
    current timestamp to create a unique payload path.
    TODO: How to retrieve the shared disk related to an organization, and not
    the first one found in the Prefect variable?

    Args:
        owner_id (str): Identifier of the owner for whom the payload path is
            generated.

    Returns:
        str: The generated payload path containing the configured shared-disk
            base path, owner identifier, and current timestamp in the
            `YYYY-MM-DD--HH-MM-SS` format.

    Raises:
        RuntimeError: If the Prefect variable cannot be retrieved or does not
            contain a suitable shared-disk configuration with an absolute path
            and `CREATE_OVERWRITE` opening mode.
    """
    if KUBERENETES_COMMON_NAMESPACE_FOR_DASK_AND_PREFECT:
        try:
            prefect_variable_result = await Variable.get(PREFECT_VAR_NAME)
        except Exception as exc:
            raise RuntimeError(
                f"Unable to load Prefect variable {PREFECT_VAR_NAME!r}",
            ) from exc
        storage_configuration = prefect_variable_result.get("storage_configuration")
        shared_disk_path = None
        for entry in storage_configuration or []:
            if not isinstance(entry, dict):
                continue
            if entry.get("kind") != "shared_disk":
                continue
            if not entry.get("name") or not entry.get("absolute_path"):
                continue
            opening_mode = entry.get("opening_mode")
            if opening_mode is None or opening_mode.upper() != "CREATE_OVERWRITE":
                continue
            shared_disk_path = entry.get("absolute_path")
            break
        if shared_disk_path is None:
            raise RuntimeError(
                f"Unable to find a shared disk path in Prefect variable {PREFECT_VAR_NAME!r}",
            )
        base = Path(shared_disk_path)
        ts = time.strftime("%Y-%m-%d--%H-%M-%S")
        payload_path = str(base / owner_id / ts)
    else:
        # Generate a hard coded path to store the payload.
        # This is a workaroud, waiting for the shared disk solution.
        # The shared disk solution will be available only when the dask workers
        # are running on the same kuberenets namespace as the prefect server / prefect workers. This
        # is due to the fact that the shared disk has to be mounted on both prefect workers and dask workers, and this
        # is only possible when they are running on the same kubernetes namespace (the PVC can't
        # be shared by 2 namespaces)
        # TODO: Once the namespace issue is resolved, we can remove this hard coded path and use the
        # shared disk path instead implemented above.
        payload_path = f"s3://prip-rs-playground/{owner_id}/{time.strftime('%Y-%m-%d--%H-%M-%S')}"
    return payload_path


async def call_dpr_flow(
    env: FlowEnvArgs,
    settings: ProcessorSettings,
    input_products: list[FlowInputProduct],
    external_variables: dict[str, Any],
    workflow: WorkflowType,
    pipeline: str | None = None,
    dask_task_timeout: int | None = None,
    temporary_folder: str | None = None,
    temporary_shared: bool = False,
) -> list[dict[str, Any]]:
    """
    Run a processor with the generic processing flow `process-generic`, then emit a `product.<product type>.generated`
    event for each generated product. When the workflow is systematic, the automations start the next processing of
    the chain from these events.

    Args:
        env: Flow environment arguments.
        settings: Default settings of the processing, see `load_processor_settings()`.
        input_products: Input products of the processor.
        external_variables: External variables of the task table (`start_datetime`, `satellite`...).
        workflow: Type of workflow of the processing.
        pipeline: Pipeline to run instead of the pipeline or unit of the settings.
        dask_task_timeout: Timeout of the Dask tasks of the processor.
        temporary_folder: Folder of the temporary files of the processor.
        temporary_shared: Whether the temporary folder is shared by the Dask workers.

    Returns:
        The STAC items of the generated products.
    """
    s3_payload: str = await generate_payload_path(env.owner_id)
    pipeline = pipeline or settings.pipeline

    a_process: DprProcessIn = DprProcessIn(
        env=env,
        processor_name=DprProcessor(settings.processor.name),
        processor_version=settings.processor.version,
        dask_cluster_label=settings.dask_cluster_name,
        s3_payload_file=f"{s3_payload}/payload_{settings.processor.name}.yaml",
        pipeline=(
            DprPipeline(pipeline) if pipeline in DprPipeline._value2member_map_ else pipeline  # pylint: disable=W0212
        ),
        unit=None if pipeline else settings.unit,
        priority=settings.priority,
        workflow_type=workflow,
        input_products=input_products,
        generated_product_to_collection_identifier=settings.generated_product_to_collection_identifier,
        auxiliary_product_to_collection_identifier=settings.auxiliary_product_to_collection_identifier,
        dask_task_timeout=dask_task_timeout,
        temporary_folder=temporary_folder,
        temporary_shared=temporary_shared,
        processing_mode=settings.processing_mode,
        **external_variables,
    )

    print(a_process.model_dump_json(indent=2))
    published_items = await dpr_processing_task(a_process)
    emit_product_generated_events(env.owner_id, published_items, workflow)
    return published_items


@task(name="dpr processing")
async def dpr_processing_task(*args, **kwargs) -> list[dict[str, Any]]:
    """See: dpr_processing"""
    return await dpr_processing.fn(*args, **kwargs)
