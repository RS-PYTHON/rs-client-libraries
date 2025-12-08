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
import re
import sys
from datetime import datetime
from importlib.metadata import version

from prefect import get_run_logger, runtime, task
from sqlalchemy import MetaData, Table, create_engine, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

DISABLE_PI = True


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

    logger = get_run_logger()
    db, engine = get_db_session()
    try:
        logger.info(f"Connecting to DB with engine: {engine}")

        metadata = MetaData()
        flow_run = Table("flow_run", metadata, autoload_with=engine)
        logger.info("Loaded flow_run table metadata")

        logger.info(f"Looking up flow_run.id for prefect_flow_id={prefect_flow_id}")
        row = db.execute(select(flow_run.c.id).where(flow_run.c.prefect_flow_id == prefect_flow_id)).fetchone()

        if row:
            logger.info(f"Found flow_run.id={row[0]} for prefect_flow_id={prefect_flow_id}")
            return row[0]
        logger.warning(f"No record found in flow_run for prefect_flow_id={prefect_flow_id}")
        return None

    except Exception as e:
        logger.error(f"Error while fetching flow_run.id for prefect_flow_id={prefect_flow_id}: {e}")
        raise
    finally:
        db.close()
        logger.info("DB session closed")


def get_pi_category_id(eopf_type: str) -> int | None:
    """
    Return id from pi_category table based on eopf_type.

    Mapping rules (example):
        - S01* -> mission='S1', name='L12-NRT'
        - S02* -> mission='S2', name='L1C'
        - S03* -> mission='S3', name='NRT'

        "S01SIWOCN": 5,  # Level-1/2 IW/GRD Sentinel-1
        "S01SIWV": 6,    # Level-2 Wave Sentinel-1
        "S02L1C": 9,     # Level-1C Sentinel-2
        "S02L2A": 10,    # Level-2A Sentinel-2
        "S03NRT": 12     # All NRT Sentinel-3
    """
    logger = get_run_logger()
    db, engine = get_db_session()

    try:
        logger.info(f"Connecting to DB with engine: {engine}")

        metadata = MetaData()
        pi_category = Table("pi_category", metadata, autoload_with=engine)
        logger.info("Loaded pi_category table metadata")

        # Determine mission and name based on eopf_type
        mission = None
        name = None
        if eopf_type.startswith("S01"):
            mission = "S1"
            name = "L12-NRT"  # Level-1/2 EW/IW/SM
        elif eopf_type.startswith("S02"):
            mission = "S2"
            name = "L1C"
        elif eopf_type.startswith("S03"):
            mission = "S3"
            name = "NRT"

        if mission is None or name is None:
            logger.warning(f"No mapping found for eopf_type={eopf_type}")
            return None

        logger.info(f"Looking up pi_category.id for mission={mission}, name={name}")
        row = db.execute(
            select(pi_category.c.id).where((pi_category.c.mission == mission) & (pi_category.c.name == name)),
        ).fetchone()

        if row:
            logger.info(f"Found pi_category.id={row[0]} for eopf_type={eopf_type}")
            return row[0]

        logger.warning(f"No record found in pi_category for eopf_type={eopf_type}")
        return None

    except Exception as e:
        logger.error(f"Error while fetching pi_category.id for eopf_type={eopf_type}: {e}")
        raise
    finally:
        db.close()
        logger.info("DB session closed")


@task(name="Record Flow Run")
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
    """Insert or update a record in flow_run table and return the DB id."""

    logger = get_run_logger()
    metadata = MetaData()
    db, engine = get_db_session()
    flow_run = Table("flow_run", metadata, autoload_with=engine)

    prefect_flow_id = runtime.flow_run.id

    # Check if record exists
    existing = db.execute(select(flow_run.c.id).where(flow_run.c.prefect_flow_id == prefect_flow_id)).fetchone()

    if not existing:
        # Insert new record with RETURNING id
        values = {
            "flow_type": resolve_param(flow_run_type, "flow_run_type", "systematic"),
            "mission": resolve_param(mission, "mission", "sentinel-1"),
            "prefect_flow_id": prefect_flow_id,
            "prefect_flow_parent_id": runtime.flow_run.parent_flow_run_id,
            "dask_version": version("dask"),
            "python_version": sys.version.split()[0],
            "dpr_processor_name": resolve_param(dpr_processor_name, "dpr_processor_name", "dpr_processor"),
            "dpr_processor_version": resolve_param(
                dpr_processor_version,
                "dpr_processor_version",
                "v1.0",
            ),
            "dpr_processor_unit": resolve_param(dpr_processor_unit, "dpr_processor_unit", "DPR_PU"),
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
        stmt = insert(flow_run).values(**values).returning(flow_run.c.id)
        flow_run_id = db.execute(stmt).scalar()
        logger.info(f"Inserted new flow_run record with id={flow_run_id}")

    else:
        flow_run_id = existing[0]
        # Update only selected fields if provided
        update_values = {}
        if start_date is not None:
            update_values["dpr_processing_start_datetime"] = start_date
        if stop_date is not None:
            update_values["dpr_processing_stop_datetime"] = stop_date
        if status is not None:
            update_values["dpr_processing_status"] = status

        if update_values:
            stmt = (
                update(flow_run)
                .where(flow_run.c.prefect_flow_id == prefect_flow_id)
                .values(**update_values)  # type: ignore
            )
            db.execute(stmt)
            logger.info(f"Updated flow_run {prefect_flow_id} with {update_values}")

    db.commit()
    logger.info(f"Successfully inserted / updated flow_run with id={flow_run_id}")
    return flow_run_id


@task(name="Record Products Realised")
def record_product_realised(flow_run_id, stac_items):
    """
    Insert STAC product metadata into the `product_realised` database table.

    This task records all products discovered during a flow run by inserting
    one row per STAC item into the `product_realised` table. Each inserted record
    includes product metadata (EOPF type, sensing time, origin date, etc.) and
    default validation flags, which will later be updated by the validation step.

    Behavior:
        - Skips insertion if no STAC items are provided.
        - Inserts new rows only (no upsert logic).
        - Marks all timing and validation flags (`unexpected`, `on_time_X_day`) as False initially.
        - Rolls back the transaction on any error to avoid partial inserts.

    Args:
        flow_run_id (str | UUID): Identifier of the current flow run, used to link inserted records.
        stac_items (list[dict]): List of STAC item dictionaries generated by DPR or discovery steps.

    Raises:
        KeyError: If a required field (e.g., `product:type`) is missing from a STAC item.
        Exception: For any other unexpected database or runtime errors.

    Logging:
        - Logs when no STAC items are provided.
        - Logs each successful insert operation.
        - Logs detailed errors on failure.

    Notes:
        - The `sensing_start_datetime` and `origin_date` fields are extracted from
          the STAC item's properties. If missing, the current timestamp (`datetime.now()`)
          is used as a fallback.
        - The `pi_category_id` is derived from the product type using `get_pi_category_id()`.
    """
    logger = get_run_logger()
    metadata = MetaData()
    db, engine = get_db_session()
    product_realised = Table("product_realised", metadata, autoload_with=engine)

    if not stac_items:
        logger.info("No STAC items provided — skipping insert into product_realised.")
        return

    try:
        for dpr_product in stac_items:
            stac_discovery = dpr_product["stac_discovery"]
            eopf_type = stac_discovery["properties"]["product:type"]

            values = {
                "flow_run_id": flow_run_id,
                "pi_category_id": get_pi_category_id(eopf_type),
                "eopf_type": eopf_type,
                "stac_item": stac_discovery,
                # get it from properties instead of product name, now() if missing
                "sensing_start_datetime": stac_discovery["properties"].get("start_datetime", datetime.now()),
                "origin_date": stac_discovery["properties"].get("datetime", datetime.now()),
                "catalog_stored_datetime": datetime.now(),
                # default to false, will be updated by validate
                "unexpected": False,
                "on_time_0_day": False,
                "on_time_1_day": False,
                "on_time_2_day": False,
                "on_time_3_day": False,
                "on_time_7_day": False,
            }
            # no upsert, only insert each element from dpr output
            stmt = insert(product_realised).values(**values)
            db.execute(stmt)
            logger.info(f"Inserted product_realised for flow_run_id={flow_run_id}")

        db.commit()

    except KeyError as ker:
        db.rollback()
        logger.error(f"Key error while unpacking DPR product: {ker}")
        raise

    except Exception as e:
        db.rollback()
        logger.error(f"Unexpected error in record_product_realised: {e}")
        raise

    finally:
        db.close()


def extract_min_datetime(list_items):
    """Finds the earliest datetime to insert in column sensing_start_datetime of product_expected."""

    datetime_patterns = [(re.compile(r"\d{8}T\d{6}"), "%Y%m%dT%H%M%S"), (re.compile(r"\d{20}"), "%Y%m%d%H%M%S%f")]
    earliest = None

    for item in list_items:
        for pattern, fmt in datetime_patterns:
            match = pattern.search(item)
            if match:
                try:
                    dt = datetime.strptime(match.group(), fmt)
                    if earliest is None or dt < earliest:
                        earliest = dt
                    break
                except ValueError:
                    continue

    return earliest


@task(name="Record Products Expected")
def record_product_expected(flow_run_id: str, dpr_processor_name, payload):
    """
    Insert expected product definitions into the `product_expected` table for a given flow run.

    This task records all product types that are expected to be generated by a specific
    DPR processor (currently only `s3_l0`). It defines each expected product type with
    its minimum and maximum expected counts and stores them in the `product_expected` table.

    Behavior:
        - Inserts one record per expected product type, if it does not already exist
          for the given `flow_run_id`.
        - Uses a hardcoded mapping of expected product types and their min/max count.
        - Derives the sensing start time from the input STAC items in the payload.

    Args:
        flow_run_id (str): Unique identifier of the current flow run.
        dpr_processor_name (str): Name of the DPR processor (e.g., `"s3_l0"`).
        payload (dict): JSON-like payload containing workflow metadata and STAC inputs/outputs.

    Raises:
        KeyError: If an expected EOPF type from the payload is not found in the lookup mapping.
        Exception: For any unexpected errors during lookup or database operations.

    Logging:
        - Logs skipped execution for unsupported processors.
        - Logs each successful product insertion.
        - Logs lookup failures or unexpected database errors.

    Notes:
        - The hardcoded mapping (`eopf_type_dict`) defines the expected product types
          and their respective min/max expected counts.
        - The sensing start datetime (`sensing_start_datetime`) is derived from
          workflow inputs using `extract_min_datetime()`.
        - The `pi_category_id` field is determined dynamically from `get_pi_category_id()`.
        - Duplicate entries for the same `(flow_run_id, eopf_type)` are not reinserted.
    """

    logger = get_run_logger()
    metadata = MetaData()
    db, engine = get_db_session()
    product_expected = Table("product_expected", metadata, autoload_with=engine)

    eopf_type_dict = []

    if dpr_processor_name in ["s3_l0", "mockup"]:
        eopf_type_dict = [
            ("S03DORDOP", 1, 1),
            ("S03DORNAV", 1, 1),
            ("S03GNSL0_", 1, 1),
            ("S03MWRL0_", 3, 3),
            ("S03OLCCR0", 0, 1),
            ("S03OLCCR1", 0, 1),
            ("S03OLCL0_", 23, 23),
            ("S03SLSL0_", 15, 22),
            ("S03ALTL0_", 12, 13),
            ("S03SRCRL0", 0, 1),
            ("S03HKML0_", 2, 2),
            ("S03NATL0_", 2, 2),
        ]
    else:
        return

    eopf_type_lookup = {k: (min_c, max_c) for k, min_c, max_c in eopf_type_dict}

    list_eopf_types = list(payload["workflow"][0]["outputs"].values())
    list_items = list((payload["workflow"][0]["inputs"]).values())
    min_val = extract_min_datetime(list_items)

    try:
        for eopf_type in list_eopf_types:

            try:
                min_c, max_c = eopf_type_lookup[eopf_type]
            except KeyError:
                logger.error(f"EOPF type '{eopf_type}' not found in eopf_type_lookup.")
                raise
            except Exception as e:
                logger.exception(f"Unexpected error accessing eopf_type_lookup with key '{eopf_type}': {e}")
                raise

            values = {
                "flow_run_id": flow_run_id,
                "pi_category_id": get_pi_category_id(eopf_type),
                "eopf_type": eopf_type,
                "sensing_start_datetime": min_val,
                "min_count": min_c,
                "max_count": max_c,
            }

            existing = db.execute(
                select(product_expected.c.id).where(
                    (product_expected.c.flow_run_id == flow_run_id) & (product_expected.c.eopf_type == eopf_type),
                ),
            ).fetchone()

            if not existing:
                stmt = insert(product_expected).values(**values)  # type: ignore
                db.execute(stmt)
                logger.info(f"Inserted product_expected for flow_run_id={flow_run_id} for eopf_type={eopf_type}")

            db.commit()

    except Exception as e:
        db.rollback()
        logger.error(f"Error in record_product_expected: {e}")
        raise
    finally:
        db.close()


@task(name="Validate Products")
def validate_products(flow_run_id: str):
    """
    Validate realised products against expected definitions for a given flow run.

    This task ensures that the products generated during a flow run (`product_realised`)
    match the products defined as expected (`product_expected`). It performs a
    consistency check between expected and realised product types and counts, updating
    the database accordingly.

    Behavior:
        - For each expected product type, counts the number of realised instances.
        - Marks missing or excess products based on defined min/max thresholds.
        - Inserts missing product records into the `product_missing` table if they are
          not already present.
        - Flags unexpected or surplus realised products in the `product_realised` table
          by setting `unexpected=True`.
        - Detects any realised product types that were not listed in the expectations
          and marks them as unexpected.

    Args:
        flow_run_id (str): Unique identifier of the current flow run.

    Raises:
        Exception: For any unexpected errors during validation or database operations.

    Logging:
        - Logs counts and validation results for each product type.
        - Logs creation of missing-product records.
        - Logs when products are marked as unexpected or already processed.
        - Logs warnings when metadata for missing products cannot be inferred.

    Notes:
        - Running this validation multiple times is idempotent; it will not duplicate
          inserts or updates.
        - The logic relies on existing rows in `product_expected` and `product_realised`.
        - `product_missing` entries are created only when a deficit is detected.
        - Products exceeding the allowed maximum or with unexpected types are flagged
          instead of deleted.
        - All changes are committed at the end; a rollback occurs on any exception.
    """

    logger = get_run_logger()
    metadata = MetaData()
    db, engine = get_db_session()

    # get all involved tables in rspy 743
    product_expected = Table("product_expected", metadata, autoload_with=engine)
    product_realised = Table("product_realised", metadata, autoload_with=engine)
    product_missing = Table("product_missing", metadata, autoload_with=engine)

    try:
        # get expected products:  type min / max count
        expected_rows = db.execute(
            select(
                product_expected.c.eopf_type,
                product_expected.c.min_count,
                product_expected.c.max_count,
            ).where(product_expected.c.flow_run_id == flow_run_id),
        ).fetchall()

        # step 1: validate each expected type
        for row in expected_rows:
            eopf_type, min_count, max_count = row

            realised_count = db.execute(
                select(func.count())  # pylint: disable = not-callable
                .select_from(product_realised)
                .where(
                    product_realised.c.flow_run_id == flow_run_id,
                    product_realised.c.eopf_type == eopf_type,
                ),
            ).scalar()

            logger.debug(f"eopf_type={eopf_type}, expected {min_count}-{max_count}, realised={realised_count}")

            if realised_count < min_count:
                # case 1 fill product_missing table
                missing_count = min_count - realised_count

                # check if already inserted
                exists_missing = db.execute(
                    select(product_missing.c.id).where(
                        product_missing.c.flow_run_id == flow_run_id,
                        product_missing.c.eopf_type == eopf_type,
                    ),
                ).fetchone()

                if not exists_missing:
                    # try to retrieve pi_category_id and sensing_start_datetime from product_realised
                    realised_info = db.execute(
                        select(
                            product_realised.c.pi_category_id,
                            product_realised.c.sensing_start_datetime,
                        )
                        .where(
                            product_realised.c.flow_run_id == flow_run_id,
                            product_realised.c.eopf_type == eopf_type,
                        )
                        .limit(1),
                    ).fetchone()

                    if realised_info:
                        pi_category_id, sensing_start_datetime = realised_info
                    else:
                        pi_category_id, sensing_start_datetime = None, None
                        logger.warning(
                            f"No realised info found for {eopf_type}, leaving category and start_datetime as NULL",
                        )

                    stmt = insert(product_missing).values(
                        flow_run_id=flow_run_id,
                        eopf_type=eopf_type,
                        count=missing_count,
                        pi_category_id=pi_category_id,
                        sensing_start_datetime=sensing_start_datetime,
                    )
                    db.execute(stmt)
                    logger.warning(
                        f"Missing products for {eopf_type}: inserted {missing_count} into product_missing "
                        f"(pi_category_id={pi_category_id}, sensing_start_datetime={sensing_start_datetime})",
                    )
                else:
                    logger.info(f"Missing products for {eopf_type} already recorded, skipping insert")

            elif realised_count > max_count:
                # case 2: update 'product_realised.unexpected'
                stmt = (
                    update(product_realised)  # type: ignore
                    .where(
                        product_realised.c.flow_run_id == flow_run_id,
                        product_realised.c.eopf_type == eopf_type,
                        product_realised.c.unexpected.is_(False),
                    )
                    .values(unexpected=True)
                )
                result = db.execute(stmt)
                if result.rowcount > 0:
                    logger.error(f"Too many products for {eopf_type}: marked all as unexpected")
                else:
                    logger.info(f"Too many products for {eopf_type} already marked, skipping update")

        # step 2: check realised types without expected
        realised_types = db.execute(
            select(product_realised.c.eopf_type).distinct().where(product_realised.c.flow_run_id == flow_run_id),
        ).fetchall()

        realised_types = [r[0] for r in realised_types]
        expected_types = [r[0] for r in expected_rows]

        extra_types = set(realised_types) - set(expected_types)
        for eopf_type in extra_types:
            stmt = (
                update(product_realised)  # type: ignore
                .where(
                    product_realised.c.flow_run_id == flow_run_id,
                    product_realised.c.eopf_type == eopf_type,
                    product_realised.c.unexpected.is_(False),
                )
                .values(unexpected=True)
            )
            result = db.execute(stmt)
            if result.rowcount > 0:
                logger.error(f"Unexpected product type {eopf_type}: marked all as unexpected")
            else:
                logger.info(f"Unexpected product type {eopf_type} already marked, skipping update")

        db.commit()

    except Exception as e:
        db.rollback()
        logger.error(f"Error in validate_products: {e}")
        raise
    finally:
        db.close()


@task(name="Update Timeliness Fields")
def update_timeliness_fields(flow_run_id):
    """
    Compute and update timeliness metrics for all realised products in a given flow run.

    This task evaluates how promptly each product was stored in the catalog after its
    origin time and updates corresponding boolean flags (`on_time_X_day`) in the
    `product_realised` table. These flags indicate whether each product met its
    expected timeliness threshold based on the category-specific maximum delay.

    Behavior:
        - Retrieves all `product_realised` records for the specified `flow_run_id`.
        - For each product, calculates the delay between `origin_date` and
          `catalog_stored_datetime`.
        - Compares the delay against the maximum allowed delay from `pi_category`
          to determine timeliness flags at 0, 1, 2, 3, and 7-day thresholds.
        - Updates the relevant fields in the database for each product.
        - Skips execution if no products are found for the given `flow_run_id`.

    Args:
        flow_run_id (str | UUID): Unique identifier of the flow run whose products should be updated.

    Raises:
        Exception: For any unexpected database or runtime errors during the update process.

    Logging:
        - Logs the computed delay for each product.
        - Logs when updates are applied or when no products are found.
        - Logs detailed errors on exceptions.

    Notes:
        - The `max_delay_seconds` value is retrieved from the associated `pi_category` entry.
        - The timeliness thresholds (0, 1, 2, 3, and 7 days) are evaluated relative
          to the product’s origin time.
        - All updates are committed at once after processing all records; a rollback
          occurs on any error.
        - Running this task multiple times simply recalculates and updates fields,
          maintaining idempotence.
    """

    logger = get_run_logger()
    metadata = MetaData()
    db, engine = get_db_session()
    pi_category = Table("pi_category", metadata, autoload_with=engine)
    product_realised = Table("product_realised", metadata, autoload_with=engine)

    try:

        products = db.execute(select(product_realised).where(product_realised.c.flow_run_id == flow_run_id)).fetchall()

        if not products:
            logger.info("No records provided — skipping updating the timeliness in product_realised.")
            return

        for prod in products:
            catalog_stored_datetime = prod.catalog_stored_datetime
            origin_datetime = prod.origin_date

            # Get the allowed max delay (in seconds)
            max_delay_seconds = db.execute(
                select(pi_category.c.max_delay_seconds).where(pi_category.c.id == prod.pi_category_id),
            ).scalar()

            delay = (catalog_stored_datetime - origin_datetime).total_seconds()
            logger.info(f"For product {prod.id} delay is {delay} seconds.")

            updates = {
                "on_time_0_day": delay <= max_delay_seconds,
                "on_time_1_day": delay <= max_delay_seconds + 1 * 24 * 3600,
                "on_time_2_day": delay <= max_delay_seconds + 2 * 24 * 3600,
                "on_time_3_day": delay <= max_delay_seconds + 3 * 24 * 3600,
                "on_time_7_day": delay <= max_delay_seconds + 7 * 24 * 3600,
            }

            db.execute(update(product_realised).where(product_realised.c.id == prod.id).values(**updates))

        db.commit()
        logger.info(f"Updated timeliness fields for flow_run_id={flow_run_id}")

    except Exception as e:
        db.rollback()
        logger.error(f"Unexpected error in update_timeliness_fields: {e}")
        raise

    finally:
        db.close()


@task(name="Record Performance Indicators")
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
    payload: dict | None = None,
    # product_realised params
    stac_items=None,
):
    """Main task that orchestrates DB recording for flow_run and product_realised."""
    if DISABLE_PI:
        return
    logger = get_run_logger()
    logger.info("Starting record_performance_indicators")

    db, _ = get_db_session()

    try:
        flow_run_id = record_flow_run(
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
        record_product_expected(flow_run_id, dpr_processor_name, payload)

        record_product_realised(flow_run_id, stac_items)  # type: ignore[unused-coroutine]

        validate_products(flow_run_id)

        update_timeliness_fields(flow_run_id)  # type: ignore[unused-coroutine]
        logger.info("Transaction committed successfully!")

    except Exception as e:
        db.rollback()
        logger.error(f"Error in record_performance_indicators: {e}")
        raise
    finally:
        db.close()
