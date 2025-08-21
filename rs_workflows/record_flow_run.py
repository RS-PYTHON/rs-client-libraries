from prefect import task, flow, get_run_logger
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import uuid, os

@task
def record_flow_run():
    """Parameters will be added, const values to be extracted from flowenv?"""
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
                flow_type="dummy_type",
                mission="dummy_mission",
                prefect_flow_id=str(uuid.uuid4()),
                prefect_flow_parent_id=None,
                dask_version="2025.0.0",
                python_version="3.11.9",
                dpr_processor_name="dummy_processor",
                dpr_processor_version="1.0",
                dpr_processor_unit="unit_test",
                dpr_processing_input_stac_items={"input": "dummy"},
                dpr_processing_start_datetime=datetime.utcnow(),
                dpr_processing_stop_datetime=datetime.utcnow(),
                dpr_processing_status="SUCCESS",
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