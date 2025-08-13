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

from pi_db_models import Base, PiCategory
from prefect import flow, get_run_logger, task
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs

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
def create_schema(engine):
    Base.metadata.create_all(engine)


@task
def insert_pi_categories(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        if session.query(PiCategory).count() == 0:
            for mission, name, desc, max_delay in PI_CATEGORY_DATA:
                session.add(PiCategory(mission=mission, name=name, description=desc, max_delay_seconds=max_delay))
            session.commit()
    finally:
        session.close()


@flow(name="PI db init")
def init_pi_database(env: FlowEnvArgs):
    # def init_pi_database():
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "init-pi-database"):

        logger.info("Start the initialisation of the tables for performance indicator database")
        db_url = (
            f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
            f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:"
            f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_PI_DB']}"
        )
        engine = create_engine(db_url)

        create_schema(engine)
        insert_pi_categories(engine)

        logger.info("End")


###########################
# Call the flow as task #
###########################


@task(name="Create pi db")
async def init_pi_db_model_task(*args, **kwargs):
    """See: search"""
    return await init_pi_database.fn(*args, **kwargs)


# if __name__ == "__main__":
#     init_pi_database()
