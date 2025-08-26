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

"""Create the database used in performance indicator"""

import os

from prefect import flow, get_run_logger, task
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.pi_db_models import Base, PiCategory

PI_CATEGORY_DATA = [
    ("S1", "L0-SEG-NRT", "Segments EW, IW, SM with timeliness NRT & PT", 1800),
    ("S1", "L0-HKGP", "HKTM & GPS", 1800),
    ("S1", "L0-WV", "Slices L0 WV", 3600),
    ("S1", "L0-NRT", "Slices L0 EW, IW, SM with timeliness NRT & PT", 5400),
    ("S1", "L12-NRT", "Slices L1 and L2 EW, IW, SM with timeliness NRT & PT", 10800),
    ("S1", "L2-WV", "Slices L2 Wave", 10800),
    ("S1", "L12-NTC", "Slices L1 and L2 EW, IW, SM with timeliness NTC", 21600),
    ("S2", "L0-HKAN", "HKTM & SADATA", 1800),
    ("S2", "L1C", "Level-1C Datastrip, Tile, and True Colour Image", 4500),
    ("S2", "L2A", "Level-2A Datastrip, Tile, and True Colour Image", 7200),
    ("S3", "HKAN", "TM_0_NAT___, TM_0_HKM___, TM_0_HKM2__", 1800),
    ("S3", "NRT", "All NRT products", 3600),
    ("S3", "STC1", "STC for SY_2_SYN and SY_2_VGP", 72000),
    ("S3", "STC2", "STC for SY_2_VG1, SY_2_VG10 and SRAL/MWR", 158400),
    ("S3", "NTC1", "NTC for all optical instruments : OLCI and SLSTR", 252000),
    ("S3", "NTC2", "NTC for SRAL and MWR", 2505600),
    ("S1", "None", "S1 product outside any PI", 0),
    ("S2", "None", "S2 product outside any PI", 0),
    ("S3", "None", "S3 product outside any PI", 0),
]


@task
def create_schema(db_url: str):
    """
    Creates all database tables defined in the pi_db_models.

    This task initializes the database schema for the Performance Indicator (PI) database
    using the provided SQLAlchemy engine.

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy database engine connected to the target database.
    """
    logger = get_run_logger()
    logger.info(f"Received: {db_url}")
    engine = create_engine(db_url)
    logger.info("Call the create_all")
    Base.metadata.create_all(engine)


@task
def insert_pi_categories(db_url: str):
    """
    Inserts default Performance Indicator (PI) categories into the database if none exist.

    This task checks whether the `pi_category` table is empty and, if so, inserts predefined
    categories from `PI_CATEGORY_DATA`.

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy database engine connected to the target database.

    Notes:
        - If categories already exist, no action is taken.
        - Commits the transaction only if new data is inserted.
    """
    engine = create_engine(db_url)
    own_session_maker = sessionmaker(bind=engine)
    session = own_session_maker()
    try:
        if session.query(PiCategory).count() == 0:
            for mission, name, desc, max_delay in PI_CATEGORY_DATA:
                session.add(PiCategory(mission=mission, name=name, description=desc, max_delay_seconds=max_delay))
            session.commit()
    finally:
        session.close()


@flow(name="PI db init")
async def init_pi_database(env: FlowEnvArgs):
    """
    Initializes the Performance Indicator (PI) database (named `performance`) schema and populates default categories.

    This Prefect flow:
      - Creates all required tables for the PI database.
      - Inserts default PI categories if none exist.

    Args:
        env (FlowEnvArgs): Prefect flow environment configuration, including runtime context variables.

    Environment Variables Required:
        POSTGRES_USER (str): PostgreSQL username.
        POSTGRES_PASSWORD (str): PostgreSQL password.
        POSTGRES_HOST (str): PostgreSQL host address.
        POSTGRES_PORT (str): PostgreSQL port.
        POSTGRES_PI_DB (str): Name of the Performance Indicator database.

    Flow Steps:
        1. Initialize flow environment and tracing span.
        2. Build database connection URL.
        3. Create database schema via `create_schema` task.
        4. Insert default PI categories via `insert_pi_categories` task.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "init-pi-database"):

        logger.info("Starting the initialization of the tables for the performance indicator database...")

        db_url = (
            f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
            f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:"
            f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_PI_DB']}"
        )
        # Prefect tasks attempt to serialize inputs (for caching, retries, mapping, etc.) and
        # also generate a cache key by hashing them. SQLAlchemy Engine contains locks
        # and connection pools (thread.RLock, weakref.ReferenceType, etc.), which are not serializable.
        # That's why instead of passing the engine object, pass only the db_url, a string, which is serializable.
        # Each task can then create its own engine locally.
        create_schema(db_url)
        insert_pi_categories(db_url)

        logger.info("The initialization of the tables for the performance indicator database finished")
