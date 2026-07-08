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

"""Use "DPR as a service" implemented by rs-dpr-service"""

import ast
import os.path as osp
import re
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path

import anyio
import yaml
from openapi_core import OpenAPI  # Spec, validate_request, validate_response

from rs_client.ogcapi.ogcapi_client import OgcApiClient
from rs_common import prefect_utils
from rs_common.utils import get_href_service

PATH_TO_YAML_OPENAPI = osp.realpath(
    osp.join(
        osp.dirname(__file__),
        "../../config",
        "staging_templates",
        "yaml",
        "dpr_openapi_schema.yaml",
    ),
)


class DprProcessor(str, Enum):
    """DPR processor name"""

    # String value = resource name in the rs-dpr-service
    S1L0 = "s1_l0"
    S3L0 = "s3_l0"
    S1ARD = "s1_ard"
    S3L1OLCI = "s3_l1olci"
    # Add S3L2OLCI as "s3_l2olci" when implementation needed (already exists in rs-dpr-service)


class DprPipeline(str, Enum):
    """DPR pipeline name"""

    # String value = resource name in the rs-dpr-service
    S1L0FULL = "s1_l0_full"
    S3L0FULL = "s3_l0_full"
    S1ARDFULL = "s1_ard_full"
    S3L1OLCIFULL = "ol1_eo"
    # Add S3L2OLCIFULL as "ol2_fr" when implementation needed (already exists in rs-dpr-service)


@dataclass
class ClusterInfo:
    """
    Information to connect to a DPR Dask cluster.

    Attributes:
        jupyter_token: JupyterHub API token. Only used in cluster mode, not local mode.
        cluster_label: Dask cluster label e.g. "dask-l0"
        cluster_instance: Dask cluster instance ID (something like "dask-gateway.17e196069443463495547eb97f532834").
        If instance is empty, the DPR processor will use the first cluster with the given label.
    """

    jupyter_token: str
    cluster_label: str
    cluster_instance: str | None = ""


default_cluster_info_repr = ClusterInfo.__repr__


def custom_cluster_info_repr(self):
    """Obfuscate the jupyter token in the string representation of the dataclass"""
    return default_cluster_info_repr(
        ClusterInfo(f"{self.jupyter_token[:8]}***", self.cluster_label, self.cluster_instance),
    )


ClusterInfo.__repr__ = custom_cluster_info_repr  # type: ignore


class DprClient(OgcApiClient):
    """Implement the OGC API client for 'DPR as a service'."""

    ##########################################
    # Override parent methods and attributes #
    ##########################################

    # Init the OpenAPI instance from config file
    openapi = OpenAPI.from_file_path(PATH_TO_YAML_OPENAPI)

    @property
    def endpoint_prefix(self) -> str:
        """Return the endpoints prefix, if any."""
        return "dpr/"

    @property
    def href_service(self) -> str:
        """
        Return the rs-dpr-service URL hostname.
        This URL can be overwritten using the RSPY_HOST_DPR_SERVICE env variable (used e.g. for local mode).
        Otherwise it should just be the RS-Server URL.
        """
        return get_href_service(self.rs_server_href, "RSPY_HOST_DPR_SERVICE")

    def get_process(  # type: ignore # pylint: disable=arguments-differ
        self,
        process_id: str,
        cluster_info: ClusterInfo,
        **kwargs,
    ) -> dict:
        """
        Call parent method with additional HTTP Get parameters.

        Args:
            process_id (str): name of the resource
            cluster_info: Information to connect to a DPR Dask cluster
        """
        return super().get_process(process_id, params=asdict(cluster_info), **kwargs)

    def run_process(
        self,
        process: str,
        cluster_info: ClusterInfo,
        s3_config_dir: str,
        payload_subpath: str,
        s3_report_dir: str | None,
        extra_data: dict | None = None,
    ) -> dict:
        """Method to start the process from rs-client - Call the endpoint /processes/{process}/execution

        Args:
            process: DPR process name
            cluster_info: Information to connect to a DPR Dask cluster
            s3_config_dir: S3 bucket folder that contains the payload and configuration files to pass to the processor
            payload_subpath: Payload file path, relative to the config folder
            s3_report_dir: S3 bucket folder were the processor report files will be written (optional). All the eopf
            local files written in the local "./reports" directory will be pushed to this S3 bucket folder.
            extra_data: Extra data to pass to the processor.

        Return:
            job_id (int, str): Returns the status code of the request + the identifier
            (or None if endpoint fails) of the running job
        """

        use_mockup = process.lower() == "mockup"

        # Data to pass to the real processor
        data = {}
        if not use_mockup:
            data = {
                "s3_config_dir": s3_config_dir,
                "payload_subpath": payload_subpath,
                "s3_report_dir": s3_report_dir,
            } | (extra_data or {})

        # For the mockup processor, read the payload yaml straight from S3.
        else:
            data = yaml.safe_load(
                prefect_utils.s3_read_bytes(osp.join(s3_config_dir, payload_subpath), _sync=True),  # type: ignore
            )

            # Add extra info
            data.update({"use_mockup": use_mockup})

        # Add the cluster info
        data.update(asdict(cluster_info))

        # Call the parent method
        return super()._run_process(process, data)

    def run_conv_safe_zarr(self, payload: dict, cluster_info: ClusterInfo):
        """Method to start the safe to zarr conversion process from rs-client -
           Call the endpoint /processes/conv_safe_zarr/execution

        Args:
            payload: Dictionary to pass to the processor,
            cluster_info: Information to connect to a DPR Dask cluster
            containing input_safe_path - the s3 path of legacy product and
            output_zarr_dir_path - the s3 path for the new zarr
        Return:
            job_id (int, str): Returns the status code of the request + the identifier
            (or None if endpoint fails) of the running job
        """
        payload.update(asdict(cluster_info))  # Add the cluster info to the payload
        return super()._run_process("conv_safe_zarr", payload)

    def stream_logs(self, url: str, logger) -> None:
        """
        Streams and forwards server-sent event (SSE) logs for a processing job.

        This method continuously connects to the job log streaming endpoint from rs-dpr-service
        and uses the provided logger to display log messages with their corresponding logging levels.
        Multi-line log entries are reconstructed before being logged. If the streaming connection
        is interrupted unexpectedly, the method automatically retries until the associated job
        reaches a terminal state or the log endpoint becomes unavailable.

        The streaming loop stops when one of the following conditions is met:
            - the job reaches a terminal state ('successful', 'failed', or 'dismissed').
            - the server closes the SSE connection after all logs have been sent.
            - the log endpoint returns HTTP 404 indicating that the job id does not exist

        Args:
            url (str): URL of the SSE endpoint used to stream the job logs.
            logger (logging.Logger): Logger instance used to emit the streamed log messages.

        Raises:
            No exceptions are propagated to the caller.
            Connection errors, streaming interruptions, and job status lookup failures are handled
            internally.
            Transient failures trigger an automatic reconnection aftera short delay, while terminal
            conditions stop the streaming loop.
            TODO: check the comment from except Exception in is_job_finished function. Maybe we
            should stop the reconnection in case of general exception ?
        """
        # regex to match start of log line (YYYY-MM-DD HH:MM:SS - LEVEL - or LEVEL:)
        log_start_pattern = re.compile(
            r"^(?:\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\s-|(?:INFO|WARNING|ERROR|DEBUG|CRITICAL):)",
        )
        # regex to extract log level
        level_pattern = re.compile(r"(?: - |^)(INFO|WARNING|ERROR|DEBUG|CRITICAL)(?: - |:)")

        def flush_log(buffer: list[str]) -> None:
            """
            Flushes the buffer of log lines to the logger.

            Args:
                buffer (list[str]): Buffer of log lines to flush.
            """
            if not buffer:
                return
            full_message = "\n".join(buffer)
            match = level_pattern.search(buffer[0])
            level = match.group(1) if match else "INFO"

            if level == "DEBUG":
                logger.debug(full_message)
            elif level == "WARNING":
                logger.warning(full_message)
            elif level in ("ERROR", "CRITICAL"):
                logger.error(full_message)
            else:
                logger.info(full_message)

            buffer.clear()

        def is_job_finished() -> bool:
            """
            Check if the job associated to the given url is finished.

            Args:
                url (str): URL of the SSE endpoint used to stream the job logs.

            Returns:
                bool: True if the job is finished, False otherwise.
            """
            try:
                # the job status can be checked directly via the parent class OgcApiClient
                job_id = url.split("/")[-2]
                status_resp = self.get_job_info(job_id)
                if isinstance(status_resp, dict) and status_resp.get("status") in ("successful", "failed", "dismissed"):
                    return True
            except Exception:  # pylint: disable=broad-exception-caught
                # maybe return True as well ? the state of protocol is unknown, but we
                # can't stream logs anymore
                logger.error("Failed to check job status. Stopping log streaming.")
                return True
            return False

        while True:  # pylint: disable=too-many-nested-blocks
            if is_job_finished():
                break

            try:
                # 86400 seconds (24h) timeout for the long running stream
                # maybe we should configure this timeout from user's input?
                logger.info(f"Connecting to log stream from {url}...")
                response = self.http_session.get(url, stream=True, **self.apikey_headers, timeout=86400)
                if response.status_code == 200:
                    buffer: list[str] = []
                    for line in response.iter_lines():
                        if line:
                            decoded = line.decode("utf-8")
                            if decoded.startswith("data: "):
                                log_line = decoded[6:]
                                if log_start_pattern.match(log_line):
                                    flush_log(buffer)
                                buffer.append(log_line)
                    flush_log(buffer)

                    # if iter_lines completes without raising an exception, it means the server
                    # cleanly closed the connection (which it does when the job finishes).
                    break
                if response.status_code == 404:
                    logger.warning("Log stream endpoint returned 404 Not Found for job. Stopping stream logs.")
                    break
                logger.warning(f"Failed to connect to stream (status {response.status_code}). Retrying in 1 second")
                time.sleep(1)
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.warning(f"Failed to stream logs ({e}). Reconnecting in 1 second")
                time.sleep(1)

    def wait_for_job(  # type: ignore
        self,
        job_status: dict,
        logger=None,
        job_name: str = "",
        poll_interval: int = 2,
    ) -> list[dict]:
        """
        Wait for job to finish.

        Returns:
            EOPF results
        """
        job_identifier = job_status.get("jobID")
        host_dpr_service = get_href_service(self.rs_server_href, "RSPY_HOST_DPR_SERVICE_PUBLIC")
        if job_identifier and logger:
            logger.warning(
                "You can cancel this DPR job by calling: "
                f"curl -X 'DELETE' '{host_dpr_service}/dpr/jobs/{job_identifier}'",
            )
            url = f"{self.href_service}/{self.endpoint_prefix}jobs/{job_identifier}/logs"
            self.stream_logs(url, logger)

        # Call parent method and parse results
        final_status = super().wait_for_job(
            job_status=job_status,
            logger=logger,
            job_name=job_name,
            poll_interval=poll_interval,
        )
        return ast.literal_eval(final_status["message"])

    ######################################################
    # These endpoints are not implemented by the service #
    ######################################################

    def get_processes(self) -> dict:
        """Get all defined processes with logging."""
        processes = super().get_processes()
        self.logger.debug("Fetched %d processes", len(processes))
        return processes

    def get_jobs(self) -> dict:
        """Get all registered jobs with logging."""
        jobs = super().get_jobs()
        self.logger.debug("Fetched %d jobs", len(jobs))
        return jobs

    def delete_job(self, _: str) -> dict:
        raise NotImplementedError

    def get_job_results(self, _: str) -> dict:
        raise NotImplementedError

    ####################
    # Specific methods #
    ####################

    async def update_configuration(
        self,
        local_path: str | Path,
        s3_path: str | Path,
        is_payload: bool = False,
        **kwargs,
    ):
        """
        Update local configuration file depending on the environment, upload it to the s3 bucket,
        and initialize output bucket folders.

        Args:
            local_path: Local configuration file path
            s3_path: S3 bucket path where to upload to modified updated configuration file
            is_payload: Specific behavior for the processor payload files
            kwargs: Specific environment variables to expand in the configuration file
        """

        to_expand = {}

        if self.local_mode:
            # In local mode, replace the S3_xxx_CLUSTER env vars, by the env vars from the ~/.s3cfg
            # config file, that contains access to the "real" s3 bucket
            to_expand = {
                "S3_ACCESSKEY_CLUSTER": "${access_key}",
                "S3_SECRETKEY_CLUSTER": "${secret_key}",
                "S3_ENDPOINT_CLUSTER": "${host_bucket}",
                "S3_REGION_CLUSTER": "${bucket_location}",
            }

        else:
            # In cluster mode, just use the "real" s3 bucket
            to_expand = {
                "S3_ACCESSKEY_CLUSTER": "${S3_ACCESSKEY}",
                "S3_SECRETKEY_CLUSTER": "${S3_SECRETKEY}",
                "S3_ENDPOINT_CLUSTER": "${S3_ENDPOINT}",
                "S3_REGION_CLUSTER": "${S3_REGION}",
            }

        # Also expand the user-given parameters
        to_expand.update(kwargs)

        # Open the input local file
        async with await anyio.open_file(str(local_path), encoding="utf-8") as opened:
            contents = await opened.read()

        # Expand the env vars as $key, ${key} or %key%
        for key, value in to_expand.items():
            for key2 in f"${key}", f"${{{key}}}", f"%{key}%":
                contents = contents.replace(key2, str(value))

        # Read the payload contents
        if is_payload:
            payload = yaml.safe_load(contents)

            # We need to create the output S3 folder with a dummy file before running DPR
            for output_product in payload["I/O"]["output_products"]:
                s3_output_dir = output_product["path"]
                s3_empty_file = f"{s3_output_dir}/.empty"
                self.logger.info(f"Write empty file: {self.logger.level} {s3_empty_file!r}")
                await prefect_utils.s3_upload_empty_file(s3_empty_file)

            # Change the dask authentication for local mode (used in old demos, could be removed)
            try:
                cluster_config = payload["dask_context"]["cluster_config"]
                if self.local_mode:
                    cluster_config["auth"] = cluster_config["auth_local_mode"]
                del cluster_config["auth_local_mode"]
            except KeyError:
                pass

            # yaml to str conversion
            contents = yaml.dump(payload, default_flow_style=False, sort_keys=False)

        # Upload the modified contents to the s3 bucket.
        return await prefect_utils.s3_upload_bytes(contents.encode("utf-8"), str(s3_path))
