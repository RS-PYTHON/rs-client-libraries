# Copyright 2024 CS Group
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
import tempfile

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

    def run_process(
        self,
        process: str,
        s3_config_dir: str,
        payload_subpath: str,
        s3_report_dir: str | None,
        use_dpr_mockup: bool = False,
    ) -> dict:
        """Method to start the process from rs-client - Call the endpoint /processes/{process}/execution

        Args:
            process: Process name
            s3_config_dir: S3 bucket folder that contains the payload and configuration files to pass to the processor
            payload_subpath: Payload file path, relative to the config folder
            s3_report_dir: S3 bucket folder were the processor report files will be written (optional). All the eopf
            local files written in the local "./reports" directory will be pushed to this S3 bucket folder.
            use_dpr_mockup: Use the real or the mockup DPR processor ?

        Return:
            job_id (int, str): Returns the status code of the request + the identifier
            (or None if endpoint fails) of the running job
        """

        # Data to pass to the real processor
        data = {}
        if not use_dpr_mockup:
            data = {
                "s3_config_dir": s3_config_dir,
                "payload_subpath": payload_subpath,
                "s3_report_dir": s3_report_dir,
            }

        # For the mockup processor, pass the payload contents.
        # Download the payload file into a temp file.
        else:
            with tempfile.NamedTemporaryFile() as temp:
                prefect_utils.s3_download_file(  # type: ignore
                    osp.join(s3_config_dir, payload_subpath),
                    temp.name,
                    _sync=True,  # type: ignore
                )

                # Read it as a yaml file
                with open(temp.name, encoding="utf-8") as opened:
                    data = yaml.safe_load(opened)

            # Add extra info
            data.update({"use_mockup": use_dpr_mockup})

        # Call the parent method
        return super()._run_process(process, data)

    def wait_for_job(self, *args, **kwargs) -> list[dict]:  # type: ignore
        """
        Wait for job to finish.

        Returns:
            EOPF results
        """
        # Call parent method and parse results
        job_status = super().wait_for_job(*args, **kwargs)
        return ast.literal_eval(job_status["message"])

    ######################################################
    # These endpoints are not implemented by the service #
    ######################################################

    def get_processes(self) -> dict:
        raise NotImplementedError

    def get_jobs(self) -> dict:
        raise NotImplementedError

    def delete_job(self, _: str) -> dict:
        raise NotImplementedError

    def get_job_results(self, _: str) -> dict:
        raise NotImplementedError

    ####################
    # Specific methods #
    ####################

    async def update_configuration(self, local_path: str, s3_path: str, is_payload: bool = False, **kwargs):
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
        with open(local_path, encoding="utf-8") as opened:
            contents = opened.read()

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

            # Change the dask authentication for local mode
            cluster_config = payload["dask_context"]["cluster_config"]
            if self.local_mode:
                cluster_config["auth"] = cluster_config["auth_local_mode"]
            del cluster_config["auth_local_mode"]

            # yaml to str conversion
            contents = yaml.dump(payload, default_flow_style=False, sort_keys=False)

        # Write the modified contents to a temp file
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(contents.encode("utf-8"))
            tmp.flush()

            # Upload the temp file to the s3 bucket
            return await prefect_utils.s3_upload_file(tmp.name, s3_path)
