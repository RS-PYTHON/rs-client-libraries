from prefect import task, get_run_logger
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
import os

@task
def record_flow_run(start_date = None, stop_date = None, status = None, runtime=None):
    """
        start_date: UTC date and time of the DPR processing starts.
        stop_date: UTC date and time of the DPR processing ends.
        status: Can be : NULL, OK, NOK. Set to ‘NULL’ by default.
        runtime: prefect context runtime variable
    """
    logger = get_run_logger()
    logger.info(f"Inserting a record into flow_run table")

    db_url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:"
        f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_PI_DB']}"
    )
    engine = create_engine(db_url, pool_pre_ping=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db = SessionLocal()

    try:
        metadata = MetaData()
        flow_run = Table("flow_run", metadata, autoload_with=engine)

        # Insert
        db.execute(
            flow_run.insert().values(
                flow_type=runtime.parameters.get("flow_run_type", "systematic"),
                mission=runtime.parameters.get("mission", "null"),
                prefect_flow_id=runtime.flow_run.id,
                prefect_flow_parent_id=runtime.flow_run.parent_flow_run_id,
                dask_version="2025.0.0",
                python_version="3.11.9",
                dpr_processor_name=runtime.parameters.get("dpr_processor_name", "dpr_processor"),
                dpr_processor_version=runtime.parameters.get("dpr_processor_version", "dpr_processor_version"),
                dpr_processor_unit=runtime.parameters.get("dpr_processor_unit", "dpr_processor_unit"),
                dpr_processing_input_stac_items=runtime.parameters.get("dpr_processing_input_stac_items", "dpr_processing_input_stac_items"),
                dpr_processing_start_datetime=start_date,
                dpr_processing_stop_datetime=stop_date,
                dpr_processing_status=status,
                excluded_from_pi=False
            )
        )

        db.commit()
        logger.info("Dummy flow_run inserted from task!")

    except Exception as e:
        db.rollback()
        logger.error(f"Failed to insert flow_run in task: {e}")
        raise
    finally:
        db.close()