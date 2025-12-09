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

"""DPR flow implementation"""


# import datetime
from os import path as osp

from prefect import get_run_logger, task

from rs_client.ogcapi.dpr_client import ClusterInfo, DprClient, DprProcessor
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs

# from rs_workflows.record_performance import record_performance_indicators


@task(name="Run DPR processor")
async def run_processor(
    env: FlowEnvArgs,
    processor: DprProcessor,
    # payload: dict,
    cluster_info: ClusterInfo,
    s3_payload_run: str,
) -> list[dict]:
    """
    Run the DPR processor.

    Args:
        env: Prefect flow environment
        processor: DPR processor name
        s3_payload_run: S3 bucket location of the output final DPR payload file.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "run-processor"):
        # record_performance_indicators(
        #     start_date=datetime.datetime.now(),
        #     status="OK",
        #     dpr_processing_input_stac_items=s3_payload_run,
        #     payload=payload,
        #     dpr_processor_name=processor.value,
        # )
        # Trigger the processor run from the dpr service
        dpr_client: DprClient = flow_env.rs_client.get_dpr_client()
        job_status = dpr_client.run_process(
            process=processor,
            cluster_info=cluster_info,
            s3_config_dir=osp.dirname(s3_payload_run),
            payload_subpath=osp.basename(s3_payload_run),
            s3_report_dir=osp.join(osp.dirname(s3_payload_run)),
        )
        dpr_job = dpr_client.wait_for_job(job_status, logger, f"{processor.value!r} processor")
        logger.info(f"DPR processor output {dpr_job}")
        # Wait for the job to finish
        # record_performance_indicators(stop_date=datetime.datetime.now(), status="OK", stac_items=dpr_job)
        return dpr_job
