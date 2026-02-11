# Copyright 2026 Airbus defence And Space
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

"""Create the Prefect flow to setup the quota monitoring database"""

import os
from urllib.parse import quote_plus

from prefect import flow, get_run_logger, task
from sqlalchemy import create_engine

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.operation.quota_monitoring_db_models import Base

DB_NAME = "s3_quota"


@task
def create_schema(db_url: str):
    """
    Creates all database tables defined in the quota_moniotirng_db_models.

    This task initializes the database schema for the Quota Monitoring database
    using the provided SQLAlchemy engine.

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy database engine connected to the target database.
    """
    logger = get_run_logger()
    engine = create_engine(db_url)
    logger.info("Call the engine to create the table for quota monitoring")
    Base.metadata.create_all(engine)


@flow(name="quota-monitoring-db-create")
async def init_quota_monitoring_database(env: FlowEnvArgs = FlowEnvArgs(owner_id="operator-quota")):
    """
    Initializes the Quota Monitoring database schema.

    This Prefect flow:
      - Creates all required tables for the Quota Monitoring database.

    Args:
        env (FlowEnvArgs): Prefect flow environment configuration, including runtime context variables.

    Environment Variables Required:
        POSTGRES_QUOTA_USER: PostgreSQL username for quota database
        POSTGRES_QUOTA_PASSWORD: PostgreSQL password
        POSTGRES_HOST (str): PostgreSQL host address.
        POSTGRES_PORT (str): PostgreSQL port.

    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "init-quota-monitoring-database"):
        password = quote_plus(os.environ["POSTGRES_QUOTA_PASSWORD"])

        db_url = (
            f"postgresql+psycopg2://{os.environ['POSTGRES_QUOTA_USER']}:"
            f"{password}@{os.environ['POSTGRES_HOST']}:"
            f"{os.environ['POSTGRES_PORT']}/" + DB_NAME
        )
        create_schema(db_url)  # type: ignore[unused-coroutine]

        logger.info("The initialization of the tables for the Quota Monitoring database finished")
