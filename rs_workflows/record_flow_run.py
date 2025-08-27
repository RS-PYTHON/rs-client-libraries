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
def record_flow_run(start_date=None, stop_date=None, status=None):
    """
    start_date: UTC date and time of the DPR processing starts.
    stop_date: UTC date and time of the DPR processing ends.
    status: Can be : NULL, OK, NOK. Set to 'NULL' by default.
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

    try:
        metadata = MetaData()
        flow_run = Table("flow_run", metadata, autoload_with=engine)

        prefect_flow_id = runtime.flow_run.id

        # Check if record exists
        existing = db.execute(select(flow_run.c.id).where(flow_run.c.prefect_flow_id == prefect_flow_id)).fetchone()

        if not existing:
            # Insert new record
            values = {
                "flow_type": runtime.flow_run.parameters.get("flow_run_type", "systematic"),
                "mission": runtime.flow_run.parameters.get("mission", "null"),
                "prefect_flow_id": prefect_flow_id,
                "prefect_flow_parent_id": runtime.flow_run.parent_flow_run_id,
                "dask_version": version("dask"),
                "python_version": sys.version.split()[0],
                "dpr_processor_name": runtime.flow_run.parameters.get("dpr_processor_name", "dpr_processor"),
                "dpr_processor_version": runtime.flow_run.parameters.get(
                    "dpr_processor_version",
                    "dpr_processor_version",
                ),
                "dpr_processor_unit": runtime.flow_run.parameters.get("dpr_processor_unit", "dpr_processor_unit"),
                "dpr_processing_input_stac_items": runtime.flow_run.parameters.get(
                    "dpr_processing_input_stac_items",
                    "dpr_processing_input_stac_items",
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
