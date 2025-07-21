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

"""Test rs-client-libraries ogcapi functions"""

import getpass
import json
from datetime import datetime

import pytest
import requests
import responses
from starlette import status

from rs_client.ogcapi.dpr_client import DprClient
from rs_client.ogcapi.ogcapi_client import OgcValidationException
from rs_client.rs_client import RsClient
from rs_common.logging import Logging

RS_SERVER_API_KEY = "RS_SERVER_API_KEY"

OWNER_ID = getpass.getuser()
TIMEOUT = 5

logger = Logging.default(__name__)

# -------------------------- Staging fixtures --------------------------


@pytest.fixture(name="dummy_href")
def get_dummy_href():
    """
    Dummy href for local_mode
    """
    dummy_href = "https://DUMMY_HREF"
    return dummy_href


@pytest.fixture(name="client")
def get_client(request, dummy_href):
    """Create a dpr or staging client."""
    client = RsClient(
        rs_server_href=dummy_href,
        rs_server_api_key=RS_SERVER_API_KEY,
        owner_id=OWNER_ID,
        logger=None,
    )
    return client.get_dpr_client() if (request.param == "dpr") else client.get_staging_client()


@pytest.fixture(name="processes_sample")
def get_processes_sample():
    """Example of response from the /processes endpoint"""
    return {
        "processes": [
            {
                "title": "string",
                "description": "string",
                "keywords": ["string"],
                "metadata": [{"title": "string", "role": "string", "href": "string"}],
                "additionalParameters": {
                    "title": "string",
                    "role": "string",
                    "href": "string",
                    "parameters": [{"name": "string", "value": ["string"]}],
                },
                "id": "string",
                "version": "string",
                "jobControlOptions": ["sync-execute"],
                "outputTransmission": ["value"],
                "links": [
                    {
                        "href": "string",
                        "rel": "service",
                        "type": "application/json",
                        "hreflang": "en",
                        "title": "string",
                    },
                ],
            },
        ],
        "links": [
            {"href": "string", "rel": "service", "type": "application/json", "hreflang": "en", "title": "string"},
        ],
    }


# -------------------------- Test endpoints --------------------------


@pytest.mark.parametrize("client", ["dpr", "staging"], indirect=True)
class TestOgcApi:
    """Parametrized pytests on DprClient and StagingClient"""

    @pytest.mark.unit
    @responses.activate
    def test_get_processes(self, client, dummy_href, processes_sample):
        """
        Test to check the behaviour of the function to get the status of a specific job
        """
        # Test not implemented
        if isinstance(client, DprClient):
            with pytest.raises(NotImplementedError):
                client.get_processes()
            return

        json_response = processes_sample
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}processes",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        # Check that the job information are returned if we specify a valid job identifier in input
        processes_resp = client.get_processes()
        assert processes_resp == json_response

        # Check that we get a validation error if the server sends a response with an unvalid format
        # (without the "links" required attribute)
        json_response.pop("links")
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/processes",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        with pytest.raises(OgcValidationException) as exc_info:
            client.get_processes()
        assert "'links' is a required property" in str(exc_info.value)

    @pytest.mark.unit
    @responses.activate
    def test_get_process(self, client, dummy_href):
        """
        Test to check the behaviour of the function to get the status of a specific job
        """
        process_id = "process_id"
        json_response = {
            "id": "EchoProcess",
            "title": "Echo Process",
            "description": "This process accepts and number of input and simple echoes each input as an output.",
            "version": "1.0.0",
            "inputs": {
                "stringInput": {
                    "title": "String Literal Input Example",
                    "description": "This is an example of a STRING literal input.",
                    "minOccurs": 1,
                    "schema": {
                        "exclusiveMaximum": False,
                        "exclusiveMinimum": False,
                        "minLength": 0,
                        "minItems": 0,
                        "uniqueItems": False,
                        "minProperties": 0,
                        "enum": ["Value1", "Value2", "Value3"],
                        "type": "string",
                        "additionalProperties": True,
                        "nullable": False,
                        "readOnly": False,
                        "writeOnly": False,
                        "deprecated": False,
                    },
                },
            },
            "outputs": {
                "stringOutput": {
                    "schema": {
                        "exclusiveMaximum": False,
                        "exclusiveMinimum": False,
                        "minLength": 0,
                        "minItems": 0,
                        "uniqueItems": False,
                        "minProperties": 0,
                        "enum": ["Value1", "Value2", "Value3"],
                        "type": "string",
                        "additionalProperties": True,
                        "nullable": False,
                        "readOnly": False,
                        "writeOnly": False,
                        "deprecated": False,
                    },
                },
            },
        }

        # ----- Check that the process information are returned if we specify a valid job identifier in input
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}processes/{process_id}",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        process_resp = client.get_process(process_id)
        assert process_resp is not None

        # ----- Check that the right error status code is returned if trying to get an unexisting resource
        process_id = "process_that_doesnt_exist"
        not_found_response = {
            "type": "https://developer.mozilla.org/en/docs/Web/HTTP/Reference/Status/404",
            "status": 404,
            "detail": '"Resource process_that_doesnt_exist not found',
        }
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}processes/{process_id}",
            json=not_found_response,
            status=status.HTTP_404_NOT_FOUND,
        )
        process_resp = client.get_process(process_id)
        assert '"Resource process_that_doesnt_exist not found' in process_resp["detail"]

        # ----- Check that we get a validation error if the server sends a response with an unvalid format
        # (e.g. we add a wrong key in the expected data)
        json_response = {
            "id": "EchoProcess",
            "title": "Echo Process",
            "description": "This process accepts and number of input and simple echoes each input as an output.",
            "version": "1.0.0",
            "inputs": {
                "stringInput": {
                    "schema": {
                        "wrong_key": False,
                    },
                },
            },
        }
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}processes/{process_id}",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        with pytest.raises(OgcValidationException) as exc_info:
            client.get_process(process_id)
        assert "{'wrong_key': False} is not valid under any of the given schemas" in str(exc_info.value)

    @pytest.mark.unit
    @responses.activate
    def test_get_jobs(self, client, dummy_href):
        """
        Test to check the behaviour of the function to get all running jobs
        """
        # Test not implemented
        if isinstance(client, DprClient):
            with pytest.raises(NotImplementedError):
                client.get_jobs()
            return

        json_response = {
            "jobs": [
                {
                    "processID": "string",
                    "type": "process",
                    "jobID": "string",
                    "status": "accepted",
                    "message": "string",
                    "created": "2019-08-24T14:15:22Z",
                    "started": "2019-08-24T14:15:22Z",
                    "finished": "2019-08-24T14:15:22Z",
                    "updated": "2019-08-24T14:15:22Z",
                    "progress": 100,
                    "links": [
                        {
                            "href": "string",
                            "rel": "service",
                            "type": "application/json",
                            "hreflang": "en",
                            "title": "string",
                        },
                    ],
                },
            ],
            "links": [
                {"href": "string", "rel": "service", "type": "application/json", "hreflang": "en", "title": "string"},
            ],
        }

        # ----- Check that the jobs information are sent if the endpoints returns a valid response
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        jobs_resp = client.get_jobs()
        assert jobs_resp == json_response

        # ----- Check that an exception is raised if the endpoints returns a status error code
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs",
            json={
                "type": "https://developer.mozilla.org/en/docs/Web/HTTP/Reference/Status/500",
                "status": 500,
                "detail": "jobs not found",
            },
            status=status.HTTP_404_NOT_FOUND,
        )
        jobs_resp = client.get_jobs()
        assert "jobs not found" in jobs_resp["detail"]

        # Check that an exception is raised if the endpoints returns an unvalid response
        # e.g. we remove the mandatory attribute "jobID"
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs",
            json=json_response["jobs"][0].pop("jobID"),  # type: ignore
            status=status.HTTP_200_OK,
        )
        with pytest.raises(OgcValidationException) as exc_info:
            client.get_jobs()
        assert "Failed to cast value to object type" in str(exc_info.value)

    @pytest.mark.unit
    @responses.activate
    def test_get_job(self, client, dummy_href):
        """
        Test to check the behaviour of the function to get the status of a specific job
        """
        job_id = "afbec9b5-7e46-4251-8e71-ec38479dbb11"
        json_response = {
            "processID": "string",
            "type": "process",
            "jobID": "string",
            "status": "accepted",
            "message": "string",
            "created": "2019-08-24T14:15:22Z",
            "started": "2019-08-24T14:15:22Z",
            "finished": "2019-08-24T14:15:22Z",
            "updated": "2019-08-24T14:15:22Z",
            "progress": 100,
            "links": [
                {"href": "string", "rel": "service", "type": "application/json", "hreflang": "en", "title": "string"},
            ],
        }
        # ----- Check that the job information are returned if we specify a valid job identifier in input
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs/{job_id}",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        job_resp = client.get_job_info(job_id)
        assert job_resp == json_response

        # ----- Check that an exception is raised if we don't specify a valid job identifier
        job_id = "0000000"
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs/{job_id}",
            json={
                "type": "https://developer.mozilla.org/en/docs/Web/HTTP/Reference/Status/404",
                "status": 404,
                "detail": "Job with ID 0000000 not found",
            },
            status=status.HTTP_404_NOT_FOUND,
        )
        job_resp = client.get_job_info(job_id)
        assert "Job with ID 0000000 not found" in job_resp["detail"]

        # ----- Check that an exception is raised if the endpoints returns an unvalid response
        # e.g. we remove the mandatory attribute "jobID"
        json_response.pop("jobID")
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs",
            json=json_response,
            status=status.HTTP_200_OK,
        )

        if isinstance(client, DprClient):
            with pytest.raises(NotImplementedError):
                client.get_jobs()
        else:
            with pytest.raises(OgcValidationException) as exc_info:
                client.get_jobs()
            assert "'jobs' is a required property" in str(exc_info.value)

    @pytest.mark.unit
    @responses.activate
    def test_delete_job(self, client, dummy_href):
        """
        Test to check the behaviour of the function to get the status of a specific job
        """
        # Test not implemented
        if isinstance(client, DprClient):
            with pytest.raises(NotImplementedError):
                client.delete_job("")
            return

        job_id = "0474d453-3306-48e2-ab32-ac00bafb3115"
        json_response = {
            "processID": "string",
            "type": "process",
            "jobID": "string",
            "status": "accepted",
            "message": "string",
            "created": "2019-08-24T14:15:22Z",
            "started": "2019-08-24T14:15:22Z",
            "finished": "2019-08-24T14:15:22Z",
            "updated": "2019-08-24T14:15:22Z",
            "progress": 100,
            "links": [
                {"href": "string", "rel": "service", "type": "application/json", "hreflang": "en", "title": "string"},
            ],
        }
        # ----- Check that the job information are returned if we specify a valid job identifier in input
        responses.add(
            method=responses.DELETE,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs/{job_id}",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        job_resp = client.delete_job(job_id)
        assert job_resp is not None

        # Check that we obtain the right error status_code when wanting to
        # delete a job with an identifier that doesn't exist
        job_id = "0000000"
        responses.add(
            method=responses.DELETE,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs/{job_id}",
            json={
                "type": "https://developer.mozilla.org/en/docs/Web/HTTP/Reference/Status/404",
                "status": 404,
                "detail": "Job with ID 0000000 not found",
            },
            status=status.HTTP_404_NOT_FOUND,
        )
        job_resp = client.delete_job(job_id)
        assert "Job with ID 0000000 not found" in job_resp["detail"]

        # ----- Check that an exception is raised if the endpoints returns an unvalid response
        # e.g. we remove the mandatory attribute "jobID"
        json_response.pop("jobID")
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        with pytest.raises(OgcValidationException) as exc_info:
            client.get_jobs()
        assert "'jobs' is a required property" in str(exc_info.value)

    @pytest.mark.unit
    @responses.activate
    def test_get_job_results(self, client, dummy_href):
        """
        Test to check the behaviour of the function to get the status of a specific job
        """
        # Test not implemented
        if isinstance(client, DprClient):
            with pytest.raises(NotImplementedError):
                client.get_job_results("")
            return

        job_id = "0474d453-3306-48e2-ab32-ac00bafb3115"
        json_response = "successful"

        # ----- Check that the job results are returned if we specify a valid job identifier in input
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs/{job_id}/results",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        job_result_resp = client.get_job_results(job_id)
        assert job_result_resp == json_response

        # ----- Check that we obtain the right error status_code when wanting to get results from unexisting job
        job_id = "0000000"
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}jobs/{job_id}/results",
            json={
                "type": "https://developer.mozilla.org/en/docs/Web/HTTP/Reference/Status/404",
                "status": 404,
                "detail": "Job with ID 0000000 not found",
            },
            status=status.HTTP_404_NOT_FOUND,
        )
        job_result_resp = client.get_job_results(job_id)
        assert "Job with ID 0000000 not found" in job_result_resp["detail"]

    # -------------------------- Test for methods used in the process endpoint --------------------------

    @pytest.mark.unit
    @responses.activate
    def test_validate_and_unmarshal_request(self, client, dummy_href):
        """
        Test to check the behaviour of the method to validate endpoints responses
        """
        process_id = "process_id"
        request_body = {
            "inputs": {"property1": "string", "property2": "string"},
            "outputs": {
                "property1": {
                    "format": {"mediaType": "string", "encoding": "string", "schema": "string"},
                    "transmissionMode": "value",
                },
                "property2": {
                    "format": {"mediaType": "string", "encoding": "string", "schema": "string"},
                    "transmissionMode": "value",
                },
            },
            "response": "raw",
        }

        # Nominal case - the json request body is valid
        request = requests.Request(
            method="POST",
            url=f"{dummy_href}/{client.endpoint_prefix}processes/{process_id}/execution",
            json=request_body,
        ).prepare()
        result = client.validate_and_unmarshal_request(request)
        assert result is not None

        # Check that we obtain an exception if the json request body is not valid
        request_body["response"] = "unauthorized_value"
        request = requests.Request(
            method="POST",
            url=f"{dummy_href}/{client.endpoint_prefix}processes/{process_id}/execution",
            json=request_body,
        ).prepare()

        with pytest.raises(OgcValidationException) as exc_info:
            client.validate_and_unmarshal_request(request)
        assert "Request body validation error" in str(exc_info.value)

    @pytest.mark.unit
    @responses.activate
    def test_validate_and_unmarshal_response(self, client, dummy_href, processes_sample):
        """
        Test to check the behaviour of the method to validate endpoints responses
        """

        # Case 1: We send a valid response from the /processes endpoint and we check
        # in that case that the response data are well returned
        json_response = processes_sample
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}processes",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        response = requests.get(url=f"{dummy_href}/{client.endpoint_prefix}processes", timeout=TIMEOUT)
        process_resp = client.validate_and_unmarshal_response(response)
        assert process_resp == json_response

        # Case 2: Let's now modify the content of the mocked response so that it becomes
        # invalid: here we remove the "links" field of the /processes response that is
        # required in the associated schema processList.yaml
        # In this case we expect an exception to be raised by the
        # validate_and_unmarshal_response() method displaying the corresonding error obtained
        # during validation

        # Generate invalid response
        json_response = processes_sample.copy()
        json_response.pop("links")
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}processes",
            json=json_response,
            status=status.HTTP_200_OK,
        )
        response = requests.get(url=f"{dummy_href}/{client.endpoint_prefix}processes", timeout=TIMEOUT)
        with pytest.raises(OgcValidationException) as exc_info:
            client.validate_and_unmarshal_response(response)
        assert "'links' is a required property" in str(exc_info.value)

        # Case 3: Let's now modify the status of the mocked response to an error status
        # status.HTTP_500_INTERNAL_SERVER_ERROR. In this case we expect an exception to be
        # raised by the validate_and_unmarshal_response() method displaying the corresonding
        # status code error
        json_response = processes_sample
        responses.add(
            method=responses.GET,
            url=f"{dummy_href}/{client.endpoint_prefix}processes",
            json=json_response,
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
        response = requests.get(url=f"{dummy_href}/{client.endpoint_prefix}processes", timeout=TIMEOUT)
        with pytest.raises(OgcValidationException) as exc_info:
            client.validate_and_unmarshal_response(response)
        assert "Unknown response http status: 500" in str(exc_info.value)

    def test_wait_for_job(self, client, mocker):
        """Test the wait_for_job function"""

        timeout = 0.3
        poll_interval = 0.1
        mock_interval = 0.15
        message = {"any": "value"}

        time1 = datetime.now()

        def patch_get_job_info(*_):
            """Patch the get_job_info function. Return success after n seconds."""
            diff = datetime.now() - time1
            if diff.total_seconds() < mock_interval:
                return {"status": "running"}
            return {"status": "successful", "message": json.dumps(message)}

        mock_job_info = mocker.patch.object(
            client,
            "get_job_info",
            side_effect=patch_get_job_info,
        )

        # Test nominal case
        time1 = datetime.now()
        if isinstance(client, DprClient):
            assert message == client.wait_for_job({"jobID": "jobID"}, logger, "job_name", timeout, poll_interval)
            assert mock_job_info.call_count == 3
        else:  # StagingClient
            client.wait_for_jobs({"job1": {"jobID": "job1"}, "job2": {"jobID": "job2"}}, logger, timeout, poll_interval)
            assert mock_job_info.call_count == 4

        # Test missing id
        with pytest.raises(Exception) as exc_info:
            client.wait_for_job({"missing": "jobID"})
        assert "Job identifier is missing" in str(exc_info.getrepr())

        # Test timeout
        mocker.patch.object(client, "get_job_info", side_effect=lambda *_: {"status": "running"})
        with pytest.raises(TimeoutError) as exc_info:
            client.wait_for_job({"jobID": "jobID"}, timeout=timeout)

        # Test failed job
        mocker.patch.object(client, "get_job_info", side_effect=lambda *_: {"status": "failed"})
        with pytest.raises(Exception) as exc_info:
            client.wait_for_job({"jobID": "jobID"})
        assert "FAILED" in str(exc_info.getrepr())

    def test_run_conv_safe_zarr(self, client, mocker):
        """Test the run_conv_safe_zarr function"""

        if not isinstance(client, DprClient):
            return  # Not applicable to StagingClient

        payload = {"input_safe_path": "s3://bucket/legacy-product", "output_zarr_dir_path": "s3://bucket/output-zarr"}
        expected_result = "mock-job-id"

        # Patch the superclass _run_process method
        mock_run_process = mocker.patch.object(
            client.__class__.__bases__[0],
            "_run_process",
            return_value=expected_result,  # get superclass
        )

        result = client.run_conv_safe_zarr(payload)

        # Assert _run_process was called correctly
        mock_run_process.assert_called_once_with("conv_safe_zarr", payload)

        # Assert return value is passed through
        assert result == expected_result
