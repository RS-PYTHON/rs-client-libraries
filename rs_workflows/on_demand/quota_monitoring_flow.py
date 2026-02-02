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

LOG_BUCKET = "rspython-ops-access-logs"
LOG_PREFIX = "prip/"
MAX_FILES = 1000
AGE_THRESHOLD = timedelta(minutes=5)
BATCH_SIZE = 500  # number of rows per batch insert


# -----------------------------
# 1. List eligible S3 log files
# -----------------------------
def list_recent_files(s3):
    """
    List up to MAX_FILES files older than AGE_THRESHOLD.
    """
    cutoff = datetime.now(timezone.utc) - AGE_THRESHOLD
    count = 0

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=LOG_BUCKET, Prefix=LOG_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            last_modified = obj["LastModified"]

            if last_modified > cutoff:
                continue

            yield key
            count += 1

            if count >= MAX_FILES:
                return


# -----------------------------
# 2. Stream-read S3 object
# -----------------------------
def read_object(s3, key):
    """
    Stream-read an S3 object line by line.
    """
    logger = get_run_logger()
    try:
        response = s3.get_object(Bucket=LOG_BUCKET, Key=key)
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
async def collect_obs_logs(env: FlowEnvArgs = FlowEnvArgs(owner_id="pcuq")):
    """
    Collect and process OVH quota monitoring logs from S3 and insert them into a PostgreSQL database.
    This async function retrieves observability logs from an S3 bucket, parses them, and performs
    batch insertions into a Postgres quota database.
    Args:
        env (FlowEnvArgs, optional): Flow environment arguments containing owner_id and other
            configuration parameters. Defaults to FlowEnvArgs(owner_id="pcuq").
    Returns:
        None
    Raises:
        psycopg2.Error: If database connection or insertion operations fail.
        KeyError: If required environment variables (POSTGRES_QUOTA_USER, POSTGRES_QUOTA_PASSWORD,
            POSTGRES_HOST, POSTGRES_PORT, S3_ENDPOINT, S3_ACCESSKEY, S3_SECRETKEY, S3_REGION)
            are not set.
        Exception: If S3 operations or log parsing encounters errors.
    Environment Variables Required:
        - POSTGRES_QUOTA_USER: PostgreSQL username for quota database
        - POSTGRES_QUOTA_PASSWORD: PostgreSQL password
        - POSTGRES_HOST: PostgreSQL host address
        - POSTGRES_PORT: PostgreSQL port number
        - S3_ENDPOINT: S3 endpoint URL
        - S3_ACCESSKEY: S3 access key ID
        - S3_SECRETKEY: S3 secret access key
        - S3_REGION: S3 region name
    Note:
        - Processes logs in batches to optimize database insertions
        - Includes OpenTelemetry instrumentation for distributed tracing
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
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["S3_ACCESSKEY"],
            aws_secret_access_key=os.environ["S3_SECRETKEY"],
            config=Config(signature_version="s3v4"),
            region_name=os.environ["S3_REGION"],
        )
        conn = psycopg2.connect(dbname="quotas_test", user=db_user, password=db_password, host=db_host, port=db_port)

        batch = []
        logger.info("⏳ Processing OVH logs with batch insert…")

        for key in list_recent_files(s3):
            logger.info(f"\n📄 Reading file: {key}")

            for line in read_object(s3, key):
                parsed = parse_log_line(line)
                if parsed:
                    batch.append(parsed)

                # Flush batch when full
                if len(batch) >= BATCH_SIZE:
                    batch_insert(conn, batch)
                    batch = []

            # Optional: delete processed file
            logger.info(f"\n📄 Deleting file: {key}")
            # s3.delete_object(Bucket=LOG_BUCKET, Key=key)

        # Final flush
        if batch:
            batch_insert(conn, batch)

        conn.close()
        print("✅ Done.")
