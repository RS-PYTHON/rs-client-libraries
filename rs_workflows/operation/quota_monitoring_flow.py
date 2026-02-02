# Copyright 2025 Airbus defence And Space
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

"""OBS logs ingestion flow implementation"""

import os
import re
from datetime import datetime, timedelta, timezone

import boto3
import botocore
import psycopg2
import psycopg2.extras
from botocore.client import Config
from prefect import flow, get_run_logger

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs

LOG_BUCKET_SUFFIX = "-access-logs"
LOG_PREFIX = "prip/"
DEFAULT_MAX_FILES = 10000
DEFAULT_AGE_THRESHOLD = 5
BATCH_SIZE = 500  # number of rows per batch insert


# -----------------------------
# 1. List eligible S3 log files
# -----------------------------
def list_recent_files(s3, platform: str, max_files: int, threshold_minute: int):
    """
    List up to 'max_files' files older than 'threshold_minute'.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=threshold_minute)
    count = 0

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=platform + LOG_BUCKET_SUFFIX, Prefix=LOG_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            last_modified = obj["LastModified"]

            if last_modified > cutoff:
                continue

            yield key
            count += 1

            if count >= max_files:
                return


# -----------------------------
# 2. Stream-read S3 object
# -----------------------------
def read_object(s3, platform, key):
    """
    Stream-read an S3 object line by line.
    """
    logger = get_run_logger()
    try:
        response = s3.get_object(Bucket=platform + LOG_BUCKET_SUFFIX, Key=key)
        for line in response["Body"].iter_lines():
            if line:
                yield line.decode("utf-8")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error reading {key}: {e}")


# -----------------------------
# 3. Log line parser (regex)
# -----------------------------
LOG_PATTERN = re.compile(
    r"(?:\S+:)?(\S+)\s+"  # bucket_owner (capture only after :)
    r"(\S+)\s+"  # bucket
    r"\[(.*?)\]\s+"  # time
    r"(\S+)\s+"  # remote_ip
    r"(?:\S+:)?(\S+)\s+"  # requester (capture only after :)
    r"(\S+)\s+"  # request_id
    r"(\S+)\s+"  # operation
    r"(\S+)\s+"  # key
    r'"(.*?)"\s+'  # request_uri
    r"(\S+)\s+"  # http_status
    r"(\S+)\s+"  # error_code
    r"(\S+)\s+"  # bytes_sent
    r"(\S+)\s+"  # object_size
    r"(\S+)\s+"  # total_time_ms
    r"(\S+)\s+"  # turnaround_time_ms
    r'"(.*?)"\s+'  # referer
    r'"(.*?)"\s+'  # user_agent
    r"(\S+)\s+"  # version_id
    r"(\S+)\s+"  # signature_version
    r"(\S+)\s+"  # authentication_type
    r"(\S+)",  # host_header
)


def parse_log_line(line: str):
    """
    Parse a single OVH/S3 access log line into a list of fields.
    """
    match = LOG_PATTERN.match(line)
    if not match:
        return None

    fields = list(match.groups())

    # Convert timestamp
    fields[2] = datetime.strptime(fields[2], "%d/%b/%Y:%H:%M:%S %z")

    # Convert numeric fields
    numeric_indices = [9, 11, 12, 13, 14]
    for idx in numeric_indices:
        fields[idx] = None if fields[idx] == "-" else int(fields[idx])

    # Replace "-" with None
    fields = [None if f == "-" else f for f in fields]

    return fields


# -----------------------------
# 4. Batch insert into PostgreSQL
# -----------------------------
INSERT_SQL = """
    INSERT INTO s3_access_log (
        bucket_owner, bucket, time, remote_ip, requester, request_id,
        operation, key, request_uri, http_status, error_code, bytes_sent,
        object_size, total_time_ms, turnaround_time_ms, referer, user_agent,
        version_id, signature_version, authentication_type, host_header
    ) VALUES %s
"""


def batch_insert(conn, rows):
    """
    Insert a batch of rows using execute_values for high performance.
    """
    if not rows:
        return

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, INSERT_SQL, rows)
    conn.commit()


# -----------------------------
# 5. Main pipeline
# -----------------------------
@flow(name="collect-obs-logs")
async def collect_obs_logs(
    platform: str = "rspython-ops",
    max_files: int = DEFAULT_MAX_FILES,
    threshold_minute: int = DEFAULT_AGE_THRESHOLD,
    env: FlowEnvArgs = FlowEnvArgs(owner_id="operator-quota"),
):
    """
    Collect and process OVH quota monitoring logs from S3 and insert them into a PostgreSQL database.
    This async function retrieves observability logs from an S3 bucket, parses them, and performs
    batch insertions into a Postgres quota database.
    Args:
        platform : platform name, will be the prefix of the bucket name
        max_files : number maximum of files read from the bucket
        threshold_minute : this threshold is set to avoid reading and deleting a log file whereas it is filled
        env (FlowEnvArgs, optional): Flow environment arguments containing owner_id and other
            configuration parameters.
    Returns:
        None
    Raises:
        psycopg2.Error: If database connection or insertion operations fail.
        KeyError: If required environment variables are not set.
        Exception: If S3 operations or log parsing encounters errors.
    Environment Variables Required:
        - POSTGRES_QUOTA_USER: PostgreSQL username for quota database
        - POSTGRES_QUOTA_PASSWORD: PostgreSQL password
        - POSTGRES_HOST: PostgreSQL host address
        - POSTGRES_PORT: PostgreSQL port number
        - S3_QUOTA_ENDPOINT: S3 endpoint URL to access the bucket with logs
        - S3_QUOTA_ACCESSKEY: S3 access key ID to access the bucket with logs
        - S3_QUOTA_SECRETKEY: S3 secret access key to access the bucket with logs
        - S3_QUOTA_REGION: S3 region name to access the bucket with logs
    """

    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "obs-quota-monitoring"):

        logger.info("Retrieve credentials to access Postgres quota database")
        db_user = os.environ["POSTGRES_QUOTA_USER"]
        db_password = os.environ["POSTGRES_QUOTA_PASSWORD"]
        db_host = os.environ["POSTGRES_HOST"]
        db_port = os.environ["POSTGRES_PORT"]

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_QUOTA_ENDPOINT"],
            aws_access_key_id=os.environ["S3_QUOTA_ACCESSKEY"],
            aws_secret_access_key=os.environ["S3_QUOTA_SECRETKEY"],
            config=Config(signature_version="s3v4"),
            region_name=os.environ["S3_QUOTA_REGION"],
        )
        conn = psycopg2.connect(dbname="quotas_test", user=db_user, password=db_password, host=db_host, port=db_port)

        batch = []
        logger.info(
            f"⏳ Processing Object Storage logs with batch insert from bucket '${platform}${LOG_BUCKET_SUFFIX}'.",
        )

        for key in list_recent_files(s3, platform, max_files, threshold_minute):
            logger.info(f"\n📄 Reading file: {key}")

            for line in read_object(s3, platform, key):
                parsed = parse_log_line(line)
                if parsed:
                    batch.append(parsed)

                # Flush batch when full
                if len(batch) >= BATCH_SIZE:
                    batch_insert(conn, batch)
                    batch = []

            # Optional: delete processed file
            logger.info(f"\n📄 Deleting file: {key}")
            s3.delete_object(Bucket=platform + LOG_BUCKET_SUFFIX, Key=key)

        # Final flush
        if batch:
            batch_insert(conn, batch)

        conn.close()
        print("✅ Done.")
