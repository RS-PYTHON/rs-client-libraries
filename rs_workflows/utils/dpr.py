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

import time
from typing import Any

from prefect import task
from prefect.variables import Variable
from pystac import ItemCollection

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
    LoggingLevel,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_workflows.on_demand_processing import dpr_processing

PREFECT_VAR_NAME = "processing-storage-configuration"
# TODO: Once the namespace issue is resolved, we can remove this hard coded path and
# use the shared disk path instead implemented in generate_payload_path().
KUBERENETES_COMMON_NAMESPACE_FOR_DASK_AND_PREFECT = False


def generate_payload_path(owner_id: str) -> str:
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
            prefect_variable_result = Variable.get(PREFECT_VAR_NAME)
        except Exception as exc:
            raise RuntimeError(
                f"Unable to load Prefect variable {PREFECT_VAR_NAME!r}",
            ) from exc
        storage_configuration = prefect_variable_result.get("storage_configuration")
        shared_disk_path = None
        for entry in storage_configuration:
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

        payload_path = f"{shared_disk_path.strip('/')}/{owner_id}/{time.strftime('%Y-%m-%d--%H-%M-%S')}"
    else:
        # Generate a hard coded path to store the payload.
        # This is a workaroud, waiting for the shared disk solution.
        # The shared disk solution will be available only when the dask workers
        # are running on the same kuberenets namespace as the prefect server / prefect workers. This
        # is due to the fact that the shared disk has to be mounted on both prefect workers and dask workers, and this
        # is only possible when they are running on the same kubernetes namespace (the PVC can't be shared by 2 namespaces).
        # TODO: Once the namespace issue is resolved, we can remove this hard coded path and use the
        # shared disk path instead implemented above.
        payload_path = f"s3://prip-rs-playground/{owner_id}/{time.strftime('%Y-%m-%d--%H-%M-%S')}"
    return payload_path


async def call_dpr_flow(
    env: FlowEnvArgs,
    input_products: list[FlowInputProduct],
    external_variables: dict[str, Any],
    dask_cluster_label: str,
    processor_name: str,
    processor_version: str,
    pipeline: DprPipeline | str | None,
    unit: str | None,
    priority: Priority | None,
    processing_mode: list[ProcessingMode],
    workflow: WorkflowType | None,
    generated_product_to_collection_identifier: list[FlowGeneratedProduct],
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping],
    logging_level: LoggingLevel = LoggingLevel.INFO,
    dask_task_timeout: int | None = None,
    temporary_folder: str | None = None,
    temporary_shared: bool = False,
) -> None:
    """
    Call any DPR processing flow with a set of default parameters.
    In case an optional parameter is not set, its value is get from Prefect Variable named 'prefect_settings'
    The payload is stored on a S3 bucket.
    """
    s3_payload: str = generate_payload_path(env.owner_id)

    a_process: DprProcessIn = DprProcessIn(
        env=env,
        processor_name=DprProcessor(processor_name),
        processor_version=processor_version,
        dask_cluster_label=dask_cluster_label,
        s3_payload_file=f"{s3_payload}/payload_{processor_name}.yaml",
        pipeline=(
            DprPipeline(pipeline) if pipeline in DprPipeline._value2member_map_ else pipeline  # pylint: disable=W0212
        ),
        unit=unit,
        priority=Priority(priority),
        workflow_type=WorkflowType(workflow),
        input_products=input_products,
        generated_product_to_collection_identifier=generated_product_to_collection_identifier,
        auxiliary_product_to_collection_identifier=auxiliary_product_to_collection_identifier,
        logging_level=logging_level,
        dask_task_timeout=dask_task_timeout,
        temporary_folder=temporary_folder,
        temporary_shared=temporary_shared,
        processing_mode=processing_mode,
        **external_variables,
    )

    print(a_process.model_dump_json(indent=2))
    await dpr_processing_task(a_process)


@task(name="dpr processing")
async def dpr_processing_task(*args, **kwargs) -> tuple[bool, ItemCollection | None]:
    """See: dpr_processing"""
    return await dpr_processing.fn(*args, **kwargs)
