from rs_client.rs_client import RsClient
from rs_client.cadip_client import CadipClient
from owslib.ogcapi.processes import Processes
import os
import requests
import json
import getpass
import pprint
import rs_common
import pprint
import sys
from rs_common.logging import Logging
from rs_common.config import ECadipStation
from pystac import Asset, Collection
from pystac import Collection, Extent, SpatialExtent, TemporalExtent
from datetime import datetime
import boto3
import botocore

TIMEOUT = 10
CATALOG_BUCKET = "rs-cluster-catalog"
RSPY_HOST_STAGING = os.environ["RSPY_HOST_STAGING"]

class RsStagingClient():
    """ Class to handle the staging process in rs-client-libraries
        
        This class provides python methods to call the different endpoints of the rs-server-staging method
        
        Remark: this class don't inherits from the owslib.ogcapi.processes.Processes class because the latter
        doesn't provide wrapping for all endpoints defined in rs-server-staging (it only provides the  /processes
        and /processes/{processId}/execution endpoints + it doesn't allow to manage apikey_header parameter which 
        is passed as an extra argument) 
    """
    def __init__(self, local_mode):# pylint: disable=too-many-arguments
        """
        Initialize the  owslib.ogcapi.processes.Processes object with provided parameters.
        """                
        # Define logger
        self.logger = Logging.default(__name__)
        self.local_mode = local_mode

    def get_processes(self):
        """
        Call endpoint /processes - Returns list of all available processes from config
        """
        return requests.get(f"{RSPY_HOST_STAGING}/processes")
    
    def get_resource(self, resource):
        """
        Call endpoint /processes/{resource} - Should return info about a specific resource
        """
        return requests.get(f"{RSPY_HOST_STAGING}/processes/{resource}")
    
    def execute_process(self, resource):
        """
        Call endpoint/processes/{resource}/execution - execute processing jobs
        Args:
            resource (str): name of the process to execute
        """
        return requests.get(f"{RSPY_HOST_STAGING}/processes/{resource}/execution")
    
    def get_job_status(self, job_id):
        """
        Call endpoint /jobs/{job_id} - get status of processing job
        Args:
            job_id (int): job identifier
        """
        return requests.get(f"{RSPY_HOST_STAGING}/jobs/{job_id}")
    
    def get_jobs(self):
        """
        Call endpoint /jobs to get the status of all jobs
        """
        return requests.get(f"{RSPY_HOST_STAGING}/jobs")
    
    def delete_job(self, job_id):
        """
        Call endpoint /jobs/{job_id} - get status of processing job
        Args:
            job_id (int): job identifier
        """
        return requests.delete(f"{RSPY_HOST_STAGING}/jobs/{job_id}")
    
    def get_specific_job_result(self, job_id):
        """
        Call endpoint /jobs/{job_id}/results - get result from a specific job
        Args:
            job_id (int): job identifier
        """
    
    def run_staging(self, api_key_header, stac_input, stac_output_coll_name, run_staging):
        """Method to start the staging process from rs-client

        Args:
            api_key_header (dict): api key to use in cluster mode
            stac_input (_type_): input information for the data to stage: either a Feature or a FeatureCollection
            stac_output_coll (_type_): _description_
        """
        staging_body = {
            "version": "0.2.0",
            "id": "staging",
            "title": {
                "en": "Staging"
            },
            "description": {
                "en": "A process that takes an external STAC ItemCollection, asynchronously download its assets into the RS catalog bucket and creates the corresponding STAC items in the RS catalog."
            },
            "jobControlOptions": [
                "async-execute"
            ],
            "keywords": [
                "stac",
                "staging"
            ],
            "links": [
                {
                    "type": "text/html",
                    "rel": "about",
                    "title": "documentation",
                    "href": "https://home.rs-python.eu/rs-documentation/rs-server/docs/doc/users/functionalities/#staging",
                    "hreflang": "en-US"
                }
            ],
            "inputs": {
                "collection": {
                    "title": "Target collection",
                    "description": "The target collection identifier in the RS catalog",
                    "id": stac_output_coll_name,
                    "schema": {
                        "type": "string"
                    },
                    "minOccurs": 1,
                    "maxOccurs": 1
                },
                "items": stac_input,
                "provider": "cadip"
            },
            "outputs": {
                "result": {
                    "title": "Output STAC items",
                    "id": "some_output_id",
                    "description": "The staged STAC ItemCollection",
                    "schema": "false",
                    "minOccurs": 1,
                    "maxOccurs": 1
                }
            }
        }
        
        # ----- Step 1 - Create the output STAC collection if it doesn't alredy exists
        self.logger.info(f"Creating a new collection {stac_output_coll_name} in the STAC catalog...")
        try:
            create_coll_response = stac_client.get_collection(stac_output_coll_name)
            self.logger.info(f"Collection {stac_output_coll_name} already exists -> staging process will use the existing one")
        except:
            create_coll_response = stac_client.add_collection(
                Collection(
                    id=stac_output_coll_name,
                    description=None,  # rs-client will provide a default description for us
                    extent=Extent(
                        spatial=SpatialExtent(bboxes=[-180.0, -90.0, 180.0, 90.0]),
                        temporal=TemporalExtent([datetime(2000, 1, 1), datetime(2030, 1, 1)]), ### To check + checker gestion de l'apikey
                    ),
                ),
            )
            self.logger.info(f"Resp status: {create_coll_response.status_code} | Message: {create_coll_response.reason}")
        
        # ----- Step 2 - Create a bucket to store staged data
        if self.local_mode:
            PREFIX = "stream/"

            s3_session = boto3.session.Session()
            s3_client = s3_session.client(
                service_name="s3",
                aws_access_key_id=os.environ["S3_ACCESSKEY"],
                aws_secret_access_key=os.environ["S3_SECRETKEY"],
                endpoint_url=os.environ["S3_ENDPOINT"],
                region_name=os.environ["S3_REGION"],
            )    
            try:
                s3_client.head_bucket(Bucket=CATALOG_BUCKET)   
                self.logger.info(f"The bucket {CATALOG_BUCKET} already exists")   
            except botocore.client.ClientError as error:            
                if int(error.response["Error"]["Code"]) == 404:
                    try:
                        s3_client.create_bucket(Bucket=CATALOG_BUCKET)
                    except botocore.exceptions.ClientError as e:
                        self.logger.info(f"Bucket CATALOG_BUCKET error: {e}")
                        sys.exit(-1)
                else:
                    self.logger.info("PANIC: Could not get bucket info. Exiting")
                    sys.exit(-1)
            # Delete all existing objects from rs-server-catalog
            if PREFIX: ### TODO - Remove ?
                response = s3_client.list_objects_v2(Bucket=CATALOG_BUCKET, Prefix=PREFIX)
                if response.get("Contents", None):
                    for object in response['Contents']:
                        self.logger.info('Deleting', object['Key'])
                        s3_client.delete_object(Bucket=CATALOG_BUCKET, Key=object['Key'])
                        
        # ----- Step 3: Launch and monitor the staging process
        
        # Remark: this class don't inherits from the owslib.ogcapi.processes.Processes class because the latter
        # doesn't provide wrapping for all endpoints defined in rs-server-staging (it only provides the  /processes
        # and /processes/{processId}/execution endpoints + it doesn't allow to manage apikey_header parameter which 
        # is passed as an extra argument) 
        post_response = requests.post(f"{RSPY_HOST_STAGING}/processes/staging/execution", 
                                    json=staging_body,
                                    **apikey_headers,
                                    timeout = TIMEOUT,)
                
        resp = json.loads(post_response.content)
        pprint.PrettyPrinter(indent=4).pprint(resp)

        job_id = resp["status"]["started"]
        print(f"\nJob ID = {job_id}\n")
        import time
        timeout = 120
        while timeout > 0:
            post_response = requests.get(f"{RSPY_HOST_STAGING}/jobs/{job_id}",
                                    **apikey_headers,
                                    timeout = TIMEOUT,)
            try:
                resp = json.loads(post_response.content)
                pprint.PrettyPrinter(indent=4).pprint(resp)
                print("\n")
                if resp["status"] == "FINISHED":
                    print("Job COMPLETED")
                    break
                    
                if resp["status"] == "FAILED":
                    print("Job FAILED")
                    break
            except (    
                    json.JSONDecodeError,
                ):        
                continue
            time.sleep(2)
            timeout -= 2

if __name__ == '__main__':
    """
    Launch staging using with rs_client library
    """
    # ----------------- Input data -----------------   
    APIKEY_HEADER = "x-api-key"
    collection_id = "cadip_s1A"
    stac_output_coll_name = "cadip_s1A_staged"
    apikey_value = None#"x-api-key"
    apikey_headers: dict = (
            {"headers": {APIKEY_HEADER: apikey_value}} if apikey_value else {}
        )
    user = os.getenv("RSPY_HOST_USER", default=getpass.getuser())
    local_mode = (os.getenv("RSPY_LOCAL_MODE") == "1")
    rs_server_href = "" if local_mode else os.getenv("RSPY_WEBSITE")
    
    # Get client instances from the generic client
    generic_client = RsClient(
        rs_server_href,
        rs_server_api_key=apikey_value,
        owner_id=user,
        logger=None,
    )
    stac_client = generic_client.get_stac_client()
    cadip_station = ECadipStation.CADIP # you can also have: INS, MPS, MTI, NSG, SGS
    cadip_client = generic_client.get_cadip_client(cadip_station)    
    
    # from datetime import datetime
    # from dateutil import parser
    # str_start = "2024-06-12T02:57:21.459000Z" # or any date sting of differing formats.
    # start_date =  parser.parse(str_start)
    # str_end = "2024-08-22T11:30:12.767000Z"
    # stop_date = parser.parse(str_end)
    #session_ids = ["S1A_20231120061537234567"]
    #cadip_search_result = cadip_client.search_sessions(session_ids)
    
    # Apply a request to get information about sessions that you want to stage
    session = requests.Session()
    href = "http://127.0.0.1:8002"
    
    ### TODO: will be replaced with a rs-client function with story https://pforge-exchange2.astrium.eads.net/jira/browse/RSPY-404
    #cadip_search_result = cadip_client.search_sessions(session_ids, start_date,stop_date)
    
    ### TODO: use the following line instead
    search_result = session.get(f"{href}/cadip/collections/{collection_id}/items").json() 
    
    # Create necessary clients to perform catalog search and staging opeation 
    staging_client = RsStagingClient(local_mode)
    # Launch staging process
    staging_client.run_staging(apikey_headers, search_result, stac_output_coll_name, stac_client)
    