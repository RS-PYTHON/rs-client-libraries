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
from importlib.metadata import version

from prefect import get_run_logger, runtime, task
from sqlalchemy import MetaData, Table, create_engine, select, update
from sqlalchemy.orm import sessionmaker


@task
def record_flow_run(
    start_date: str = None,
    stop_date: str = None,
    status: str = None,
    flow_run_type: str = None,
    mission: str = None,
    dpr_processor_name: str = None,
    dpr_processor_version: str = None,
    dpr_processor_unit: str = None,
    dpr_processing_input_stac_items: str = None,
):
    """
    Records or updates flow_run in database with priority:
    1. Explicit task parameter
    2. runtime.flow_run.parameters
    3. Hardcoded default
    """

    logger = get_run_logger()
    logger.info("Inserting or updating a record in flow_run table")

    db_url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:"
        f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_PI_DB']}"
    )
    engine = create_engine(db_url, pool_pre_ping=True)
    db = sessionmaker(bind=engine, autoflush=False, autocommit=False)()

    def resolve_param(param_value, runtime_key, default):
        """Return param_value if set, else runtime parameter, else default."""
        if param_value is not None:
            return param_value
        runtime_val = runtime.flow_run.parameters.get(runtime_key)
        return runtime_val if runtime_val is not None else default

    try:
        metadata = MetaData()
        flow_run = Table("flow_run", metadata, autoload_with=engine)

        prefect_flow_id = runtime.flow_run.id

        # Check if record exists
        existing = db.execute(
            select(flow_run.c.id).where(flow_run.c.prefect_flow_id == prefect_flow_id)
        ).fetchone()

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
                "dpr_processor_version": resolve_param(dpr_processor_version, "dpr_processor_version", "dpr_processor_version"),
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

        db.commit()
        logger.info("Successfully committed transaction to flow_run!")

    except Exception as e:
        db.rollback()
        logger.error(f"Failed to insert/update flow_run in task: {e}")
        raise
    finally:
        db.close()
