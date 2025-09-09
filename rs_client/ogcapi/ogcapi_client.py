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

"""Use OGC API services with rs-client-libraries"""

import json
import math
import time
from typing import Any

# openapi_core libraries used for endpoints validation
import requests
from openapi_core import OpenAPI  # Spec, validate_request, validate_response
from openapi_core.contrib.requests import (
    RequestsOpenAPIRequest,
    RequestsOpenAPIResponse,
)
from requests import Response
from requests.models import PreparedRequest

from rs_client.rs_client import TIMEOUT, RsClient


class OgcValidationException(Exception):
    """
    Exception raised when an error occurs during the OGC validation of the endpoints
    """


class OgcApiClient(RsClient):
    """
    Class to handle OGC API processes in rs-client-libraries

    This class provides python methods to call the different endpoints of the OGC API service.

    Remark: this class don't inherits from the owslib.ogcapi.processes.Processes class because the latter
    doesn't provide wrapping for all endpoints defined in our services (it only provides the  /processes
    and /processes/{processId}/execution endpoints + it doesn't allow to manage apikey_header parameter which
    is passed as an extra argument).
    """

    @property
    def endpoint_prefix(self) -> str:
        """Return the endpoints prefix, if any."""
        return ""  # no prefix by default

    @classmethod
    def get_openapi(cls) -> OpenAPI:
        """Return the OpenAPI instance of the subclass"""
        return cls.openapi  # type: ignore # pylint: disable=no-member

    def validate_and_unmarshal_request(self, request: PreparedRequest) -> Any:
        """Validate an endpoint request according to the ogc specifications

        Args:
            request (Request): endpoint request

        Returns:
            RequestUnmarshalResult.data: data validated by the openapi_core
            unmarshal_response method
        """
        openapi_request = RequestsOpenAPIRequest(request)

        # validate request
        result = self.get_openapi().unmarshal_request(openapi_request)

        if result.errors:
            raise OgcValidationException(
                f"Error validating the request of the endpoint "
                f"{openapi_request.path}: {', '.join(str(x) for x in result.errors)}",
            )
        if not result.body:
            raise OgcValidationException(
                f"Error validating the request of the endpoint "
                f"{openapi_request.path}: 'data' field of RequestUnmarshalResult"
                f"object is empty",
            )
        return result.body

    def validate_and_unmarshal_response(self, response: Response) -> Any:
        """
        Validate an endpoint response according to the ogc specifications
        (described as yaml schemas)

        Args:
            response (Response): endpoint response
        Returns:
            ResponseUnmarshalResult.data: data validated by the openapi_core
            unmarshal_response method
        """
        openapi_request = RequestsOpenAPIRequest(response.request)
        openapi_response = RequestsOpenAPIResponse(response)

        # validate response
        result = self.get_openapi().unmarshal_response(openapi_request, openapi_response)  # type: ignore
        if result.errors:
            raise OgcValidationException(  # type: ignore
                f"Error validating the response of the endpoint {openapi_request.path} - "
                f"Server response content: {response.json()} - "
                f"Validation error of the server response: {', '.join(str(x) for x in result.errors)}",
            )
        if not result.data:
            raise OgcValidationException(
                f"Error validating the response of the endpoint "
                f"{openapi_request.path}: 'data' field of ResponseUnmarshalResult"
                f"object is empty",
            )
        return json.loads(response.content)

    ##################################
    # Call OGC API service endpoints #
    ##################################

    def get_processes(self) -> dict:
        """_summary_

        Returns:
            dict: dictionary containing the content of the response
        """
        response = self.http_session.get(
            url=f"{self.href_service}/{self.endpoint_prefix}processes",
            timeout=TIMEOUT,
            **self.apikey_headers,
        )
        return self.validate_and_unmarshal_response(response)

    def get_process(self, process_id: str) -> dict:
        """
        Wrapper to get a specific process
        Args:
            process_id (str): name of the resource
        """
        response = self.http_session.get(
            url=f"{self.href_service}/{self.endpoint_prefix}processes/{process_id}",
            timeout=TIMEOUT,
            **self.apikey_headers,
        )
        return self.validate_and_unmarshal_response(response)

    def _run_process(self, process: str, body: dict) -> dict:
        """Method to start the process from rs-client - Call the endpoint /processes/{process}/execution

        Args:
            process: Process name
            body: POST HTTP request body as a JSON dict

        Return:
            job_id (int, str): Returns the status code of the request + the identifier
            (or None if endpoint fails) of the running job
        """
        # Check that the request containing the body is valid
        request = requests.Request(
            method="POST",
            url=f"{self.href_service}/{self.endpoint_prefix}processes/{process}/execution",
            json=body,
        ).prepare()

        # Validate the body of the request that will be sent to the service
        self.validate_and_unmarshal_request(request)

        response = self.http_session.post(
            url=f"{self.href_service}/{self.endpoint_prefix}processes/{process}/execution",
            json=body,
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(response)

    def get_jobs(self) -> dict:
        """Method to get running jobs"""
        response = self.http_session.get(
            url=f"{self.href_service}/{self.endpoint_prefix}jobs",
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(response)

    def get_job_info(self, job_id: str) -> dict:  # pylint: disable=too-many-locals
        """Method to get a specific job response"""
        response = self.http_session.get(
            url=f"{self.href_service}/{self.endpoint_prefix}jobs/{job_id}",
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(response)

    def delete_job(self, job_id: str) -> dict:  # pylint: disable=too-many-locals
        """Method to get a specific job response"""
        response = self.http_session.delete(
            url=f"{self.href_service}/{self.endpoint_prefix}jobs/{job_id}",
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(response)

    def get_job_results(self, job_id: str) -> dict:
        """Wrapper to get the result of a specfific job

        Args:
            job_id (str): _description_
        """
        response = self.http_session.get(
            url=f"{self.href_service}/{self.endpoint_prefix}jobs/{job_id}/results",
            timeout=TIMEOUT,
            **self.apikey_headers,
        )
        return self.validate_and_unmarshal_response(response)

    def wait_for_job(
        self,
        job_status: dict,
        logger=None,
        job_name: str = "",
        # timeout: int | float = math.inf,
        poll_interval: int = 2,
    ) -> dict:
        """
        Wait for job to finish.

        Args:
            job_status: Returned by `_run_process`
            logger: To show advancement in logger
            job_name: Job name to show in the logger
            timeout: Job completion timeout in seconds.
                        NOTE: This argument has been disabled, see the comment bellow !
            poll_interval: When to check again for job completion in seconds

        Returns:
            Job status

        Raises:
            RuntimeError in case of error
        """
        # This parameter has been disabled as input, requiring the client to use the default timeout (INFINITE)
        # for the entire staging process.
        # The reason behind is that the rs-server staging module does not currently provide a 'cancel_job(job_id)'
        # endpoint. The staging process only stops if at least one file fails to stage. In that case, it sends
        # 'dask_client.cancel_all()' to terminate all the running tasks in the dask cluster, waits for them to
        # finish, cleans up (deletes) already staged files, and marks the job status as failed. The rs-client then
        # retrieves this failed status through 'self.get_job_info(job_identifier)' (see below in this function).
        # Allowing 'timeout' as an input argument would be error-prone. If a client sets a timeout shorter than
        # the actual staging duration, the rs-client would raise a timeout error even if the staging
        # could eventually succeed.
        # In such cases, the rs-client should ideally be able to notify the staging process to stop by
        # calling 'cancel_job(job_id)', but this endpoint has not yet been implemented in the rs-server staging module.
        # For now, the only option available to the rs-client is to wait until the staging process finishes, whether
        # it ends in success or failure.
        # If the 'cancel_job(job_id)' endpoint shall be implemented in the staging module, simply uncomment the
        # input parameter timeout and delete / comment the following line. All the other functions that are using
        # this function shall be matter to a change as well (uncomment the timeout parameter)
        timeout: int | float = math.inf
        try:
            status_type = ""
            job_identifier = job_status.get("jobID")
            if not job_identifier:
                raise RuntimeError("Job identifier is missing.")

            while True:
                job_status = self.get_job_info(job_identifier)
                if logger:
                    logger.info(f"job_status: {job_status}")
                status_type = job_status.get("status", "")
                if logger:
                    logger.info(
                        f"----- {job_name} job {job_identifier!r}: {status_type.upper()} \n",
                    )

                # Exit the loop
                if status_type in {"successful", "failed", "dismissed"}:
                    break

                # Or sleep n seconds and try again
                timeout -= poll_interval
                # This will make sense only when the 'timeout' shall be activated as input argument
                # (see the comment) from the beginning of this function
                if timeout <= 0:
                    raise TimeoutError(f"Timed out while waiting for {job_name} job {job_identifier!r}")
                time.sleep(poll_interval)

        # Log all exceptions except the timeout
        except TimeoutError:
            raise
        except Exception as e:
            raise RuntimeError(f"Exception while monitoring job {job_name}: {job_status}") from e

        if status_type == "successful":
            if logger:
                logger.info(f"----- {job_name} job {job_identifier!r}: COMPLETED \n")
            return job_status

        raise RuntimeError(f"{job_name} job {job_identifier!r}: FAILED")
