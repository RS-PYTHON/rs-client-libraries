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

from sqlalchemy import (
    create_engine, Column, BigInteger, Integer, String, Boolean,
    Text, TIMESTAMP, JSON, ForeignKey, CheckConstraint
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from prefect import flow, get_run_logger, task

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
import os

Base = declarative_base()

class FlowRun(Base):
    __tablename__ = "flow_run"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    flow_type = Column(Text)
    mission = Column(Text)
    prefect_flow_id = Column(UUID(as_uuid=True))
    prefect_flow_parent_id = Column(UUID(as_uuid=True))
    dask_version = Column(Text)
    python_version = Column(Text)
    dpr_processor_name = Column(Text)
    dpr_processor_version = Column(Text)
    dpr_processor_unit = Column(Text)
    dpr_processing_input_stac_items = Column(JSONB)
    dpr_processing_start_datetime = Column(TIMESTAMP)
    dpr_processing_stop_datetime = Column(TIMESTAMP)
    dpr_processing_status = Column(Text)
    excluded_from_pi = Column(Boolean, default=False)

    products_expected = relationship("ProductExpected", back_populates="flow_run", cascade="all, delete")
    products_realised = relationship("ProductRealised", back_populates="flow_run", cascade="all, delete")
    products_missing = relationship("ProductMissing", back_populates="flow_run", cascade="all, delete")


class PiCategory(Base):
    __tablename__ = "pi_category"

    id = Column(Integer, primary_key=True, autoincrement=True)
    mission = Column(Text)
    name = Column(Text)
    description = Column(Text)
    max_delay_seconds = Column(BigInteger)

    products_expected = relationship("ProductExpected", back_populates="pi_category")
    products_realised = relationship("ProductRealised", back_populates="pi_category")
    products_missing = relationship("ProductMissing", back_populates="pi_category")


class ProductExpected(Base):
    __tablename__ = "product_expected"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    flow_run_id = Column(BigInteger, ForeignKey("flow_run.id", ondelete="CASCADE", onupdate="CASCADE"))
    pi_category_id = Column(Integer, ForeignKey("pi_category.id"))
    eopf_type = Column(Text)
    sensing_start_datetime = Column(TIMESTAMP)
    min_count = Column(Integer)
    max_count = Column(Integer)

    __table_args__ = (
        CheckConstraint("min_count >= 0"),
        CheckConstraint("max_count >= min_count"),
    )

    flow_run = relationship("FlowRun", back_populates="products_expected")
    pi_category = relationship("PiCategory", back_populates="products_expected")


class ProductRealised(Base):
    __tablename__ = "product_realised"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    flow_run_id = Column(BigInteger, ForeignKey("flow_run.id", ondelete="CASCADE", onupdate="CASCADE"))
    pi_category_id = Column(Integer, ForeignKey("pi_category.id"))
    eopf_type = Column(Text)
    stac_item = Column(JSONB)
    sensing_start_datetime = Column(TIMESTAMP)
    origin_date = Column(TIMESTAMP)
    catalog_stored_datetime = Column(TIMESTAMP)
    unexpected = Column(Boolean, default=False)
    on_time_0_day = Column(Boolean, default=False)
    on_time_1_day = Column(Boolean, default=False)
    on_time_2_day = Column(Boolean, default=False)
    on_time_3_day = Column(Boolean, default=False)
    on_time_7_day = Column(Boolean, default=False)

    flow_run = relationship("FlowRun", back_populates="products_realised")
    pi_category = relationship("PiCategory", back_populates="products_realised")


class ProductMissing(Base):
    __tablename__ = "product_missing"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    flow_run_id = Column(BigInteger, ForeignKey("flow_run.id", ondelete="CASCADE", onupdate="CASCADE"))
    pi_category_id = Column(Integer, ForeignKey("pi_category.id"))
    eopf_type = Column(Text)
    sensing_start_datetime = Column(TIMESTAMP)
    count = Column(Integer)

    __table_args__ = (
        CheckConstraint("count >= 0"),
    )

    flow_run = relationship("FlowRun", back_populates="products_missing")
    pi_category = relationship("PiCategory", back_populates="products_missing")


PI_CATEGORY_DATA = [
    ('S1', 'L0-SEG-NRT', 'Segments EW, IW, SM with timeliness NRT & PT', 1800),
    ('S1', 'L0-HKGP', 'HKTM & GPS', 1800),
    ('S1', 'L0-WV', 'Slices L0 WV', 3600),
    ('S1', 'L0-NRT', 'Slices L0 EW, IW, SM with timeliness NRT & PT', 5400),
    ('S1', 'L12-NRT', 'Slices L1 and L2 EW, IW, SM with timeliness NRT & PT', 10800),
    ('S1', 'L2-WV', 'Slices L2 Wave', 10800),
    ('S1', 'L12-NTC', 'Slices L1 and L2 EW, IW, SM with timeliness NTC', 21600),
    ('S2', 'L0-HKAN', 'HKTM & SADATA', 1800),
    ('S2', 'L1C', 'Level-1C Datastrip, Tile, and True Colour Image', 4500),
    ('S2', 'L2A', 'Level-2A Datastrip, Tile, and True Colour Image', 7200),
    ('S3', 'HKAN', 'TM_0_NAT___, TM_0_HKM___, TM_0_HKM2__', 1800),
    ('S3', 'NRT', 'All NRT products', 3600),
    ('S3', 'STC1', 'STC for SY_2_SYN and SY_2_VGP', 72000),
    ('S3', 'STC2', 'STC for SY_2_VG1, SY_2_VG10 and SRAL/MWR', 158400),
    ('S3', 'NTC1', 'NTC for all optical instruments : OLCI and SLSTR', 252000),
    ('S3', 'NTC2', 'NTC for SRAL and MWR', 2505600),
    ('S1', 'None', 'S1 product outside any PI', 0),
    ('S2', 'None', 'S2 product outside any PI', 0),
    ('S3', 'None', 'S3 product outside any PI', 0)
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
                session.add(PiCategory(
                    mission=mission,
                    name=name,
                    description=desc,
                    max_delay_seconds=max_delay
                ))
            session.commit()
    finally:
        session.close()

@flow(name="PI db model")
def init_pi_database(env: FlowEnvArgs):
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "init-pi-database"):

        logger.info("Start the initialisation of the tables for performance indicator database")
        db_url = f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_PI_DB']}"
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
