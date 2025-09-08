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

"""Module with task used to insert or update flow run table."""

import os
import sys
from datetime import datetime
from importlib.metadata import version

from prefect import get_run_logger, runtime, task
from sqlalchemy import MetaData, Table, create_engine, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker


def get_db_session():
    """Initialize and return a DB session."""
    try:
        db_url = (
            f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
            f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:"
            f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_PI_DB']}"
        )
    except KeyError as e:
        # Fail fast with a clearer error message
        raise KeyError(f"Missing environment variable for DB connection: {e}") from e

    engine = create_engine(db_url, pool_pre_ping=True)
    session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return session(), engine


def resolve_param(param_value, runtime_key, default):
    """Return param_value if set, else runtime parameter, else default."""
    if param_value is not None:
        return param_value
    runtime_val = runtime.flow_run.parameters.get(runtime_key)
    return runtime_val if runtime_val is not None else default

def get_flow_run_id(prefect_flow_id: str) -> int | None:
    """Return id from flow_run table for given prefect_flow_id."""
    db, engine = get_db_session()
    try:
        metadata = MetaData()
        flow_run = Table("flow_run", metadata, autoload_with=engine)

        row = db.execute(
            select(flow_run.c.id).where(flow_run.c.prefect_flow_id == prefect_flow_id)
        ).fetchone()

        return row[0] if row else None
    finally:
        db.close()

def record_flow_run(
    start_date: datetime | str | None = None,
    stop_date: datetime | str | None = None,
    status: str | None = None,
    flow_run_type: str | None = None,
    mission: str | None = None,
    dpr_processor_name: str | None = None,
    dpr_processor_version: str | None = None,
    dpr_processor_unit: str | None = None,
    dpr_processing_input_stac_items: str | None = None,
):
    """Insert or update a record in flow_run table."""
    logger = get_run_logger()
    metadata = MetaData()
    db, engine = get_db_session()
    flow_run = Table("flow_run", metadata, autoload_with=engine)

    prefect_flow_id = runtime.flow_run.id

    # Check if record exists
    existing = db.execute(select(flow_run.c.id).where(flow_run.c.prefect_flow_id == prefect_flow_id)).fetchone()

    if not existing:
        # Insert new record
        values = {
            "flow_type": resolve_param(flow_run_type, "flow_run_type", "systematic"),
            "mission": resolve_param(mission, "mission", "sentinel-1"),
            "prefect_flow_id": prefect_flow_id,
            "prefect_flow_parent_id": runtime.flow_run.parent_flow_run_id,
            "dask_version": version("dask"),
            "python_version": sys.version.split()[0],
            "dpr_processor_name": resolve_param(dpr_processor_name, "dpr_processor_name", "dpr_processor"),
            "dpr_processor_version": resolve_param(
                dpr_processor_version, "dpr_processor_version", "dpr_processor_version",
            ),
            "dpr_processor_unit": resolve_param(dpr_processor_unit, "dpr_processor_unit", "dpr_processor_unit"),
            "dpr_processing_input_stac_items": resolve_param(
                dpr_processing_input_stac_items,
                "dpr_processing_input_stac_items",
                "{'dpr_processing_input_stac_items': 'value'}",
            ),
            "dpr_processing_start_datetime": start_date,
            "dpr_processing_stop_datetime": stop_date,
            "dpr_processing_status": status,
            "excluded_from_pi": False,
        }
        db.execute(flow_run.insert().values(**values))
        logger.info("Inserted new flow_run record")
    else:
        # Update only selected fields if provided
        update_values = {}
        if start_date is not None:
            update_values["dpr_processing_start_datetime"] = start_date
        if stop_date is not None:
            update_values["dpr_processing_stop_datetime"] = stop_date
        if status is not None:
            update_values["dpr_processing_status"] = status

        if update_values:
            stmt = update(flow_run).where(flow_run.c.prefect_flow_id == prefect_flow_id).values(**update_values)
            db.execute(stmt)
            logger.info(f"Updated flow_run {prefect_flow_id} with {update_values}")


def record_product_realised():
    """Upsert into product_realised table (insert or update)."""

    logger = get_run_logger()
    metadata = MetaData()
    db, engine = get_db_session()
    product_realised = Table("product_realised", metadata, autoload_with=engine)
    flow_run_id = get_flow_run_id(runtime.flow_run.id)
    values = {
        "flow_run_id": flow_run_id,
        "pi_category_id": 1,
        "eopf_type": "EOPF_TYPE",
        "stac_item": {"example": "stac_item"},
        "sensing_start_datetime": datetime.now(),
        "origin_date": datetime.now(),
        "catalog_stored_datetime": datetime.now(),
        "unexpected": False,
        "on_time_0_day": True,
        "on_time_1_day": False,
        "on_time_2_day": False,
        "on_time_3_day": False,
        "on_time_7_day": False,
    }

    stmt = insert(product_realised).values(**values)
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=["flow_run_id"],  # conflict key
        set_={k: v for k, v in values.items() if k != "flow_run_id"},  # update only these
    )

    db.execute(upsert_stmt)
    logger.info(f"Upserted product_realised for flow_run_id={flow_run_id}")


@task
def record_performance_indicators(
    # flow_run params
    start_date: datetime | str | None = None,
    stop_date: datetime | str | None = None,
    status: str | None = None,
    flow_run_type: str | None = None,
    mission: str | None = None,
    dpr_processor_name: str | None = None,
    dpr_processor_version: str | None = None,
    dpr_processor_unit: str | None = None,
    dpr_processing_input_stac_items: str | None = None,
    # product_realised params
    product_id: str | None = None,
    product_type: str | None = None,
    product_status: str | None = None,
):
    """Main task that orchestrates DB recording for flow_run and product_realised."""

    logger = get_run_logger()
    logger.info("Starting record_performance_indicators")

    db, _ = get_db_session()

    try:
        record_flow_run(
            start_date,
            stop_date,
            status,
            flow_run_type,
            mission,
            dpr_processor_name,
            dpr_processor_version,
            dpr_processor_unit,
            dpr_processing_input_stac_items,
        )
        db.commit() # temp

        record_product_realised()
        db.commit()
        logger.info("Transaction committed successfully!")

    except Exception as e:
        db.rollback()
        logger.error(f"Error in record_performance_indicators: {e}")
        raise
    finally:
        db.close()
