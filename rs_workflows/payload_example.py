import yaml
from pathlib import Path
from payload_template import (
    PayloadSchema, GeneralConfiguration,
    ExternalModule, Breakpoints, WorkflowStep,
    IOConfig, InputProduct, OutputProduct, AdfConfig,
    DaskContext, EOQCConfig, StoreParams
)

# -------------------------------
# Dynamic input
# -------------------------------
processor_name = "s1-ard"

# Create a workflow step instance using the schema
workflow_step = WorkflowStep(
    name=processor_name,
    active=True,
    validate_output=False,
    module="eopf.computing.utils",
    processing_unit="EORechunkingUnit",
    inputs={"l1a": "MSI"},
    adfs={"dem": "DEM"},
    outputs={
        "l1a": "output_l1a_internal",
        "l1b_.*": "output_folder",
        "l1b_intermediate": "cache"
    },
    parameters={
        "value": 0.5,
        "variable_path": "/measurements/stuff",
        "variable_name": "stiff",
        "shape": [1500]
    }
)

# -------------------------------
# Build the full payload using the schema
# -------------------------------
payload = PayloadSchema(
    dotenv=["$EOPF_CONFIG/.env_test"],
    general_configuration=GeneralConfiguration(
        logging={"level": "DEBUG"},
        triggering__use_basic_logging=True,
        triggering__wait_before_exit=90,
        dask__export_graphs="./graphs/",
        breakpoints__folder="./breakpoints",
        triggering__create_temporary=True,
        triggering__temporary_shared=True,
        triggering__validate_run=True,
        triggering__validate_mode="STAC",
        triggering__error_policy="FAIL_FAST",
        temporary__folder="s3::${S3_OUTPUT_TEST_DATA_PATH}/temporary",
        temporary__folder_s3_secret="test_data",
        temporary__folder_create_folder=True,
        triggering__dask_monitor__enabled=True,
        triggering__dask_monitor__cancel=True,
        triggering__dask_monitor__cancel_state="PAUSED | STUCK_SPILL"
    ),
    external_modules=[
        ExternalModule(name="math", alias="m", nested=True),
        ExternalModule(name="empty_test_store", folder="./tests/store")
    ],
    breakpoints=Breakpoints(
        all=False,
        ids=["example_breakpoint_1"],
        folder="./breakpoints",
        store_params=StoreParams(
            storage_options={
                "key": "<key>",
                "secret": "<secret>",
                "client_kwargs": {
                    "endpoint_url": "<url>",
                    "region_name": "<region>"
                }
            }
        )
    ),
    workflow=[workflow_step],
    io=IOConfig(
        input_products=[
            InputProduct(id="OLCI", path="s3::zip::/mnt/1TERA/EOPF/Test_data_unit/S3A_OL_1_EFR.zip",
                         store_type="safe", store_params=StoreParams(multiplicity="exactly_one")),
            InputProduct(id="SLICES", path="zip::/mnt/1TERA/EOPF/Test_data_unit/", store_type="safe",
                         type="regex", store_params=StoreParams(regex="S3A_OL*", multiplicity="exactly_one"))
        ],
        adfs=[AdfConfig(id="DEM", path="zip::/mnt/1TERA/EOPF/ADFS/DEM.zarr", store_params=StoreParams())],
        output_products=[
            OutputProduct(id="output", path="output.zarr", type="filename", opening_mode="CREATE_OVERWRITE",
                          store_type="zarr", store_params=StoreParams()),
            OutputProduct(id="outputs_l1c", path="./finals_l1c/", type="folder", opening_mode="CREATE_OVERWRITE",
                          store_type="zarr", store_params=StoreParams())
        ]
    ),
    dask_context=DaskContext(
        cluster_type="local",
        address="http://scheduler",
        cluster_config={"n_workers": 6, "threads_per_worker": 1},
        client_config={},
        performance_report_file="report.html",
        dask_config={"distributed.worker.local_directory": "~/eopf/output"}
    ),
    logging=["$EOPF_ROOT/logging/conf/default.json"],
    config=["$EOPF_ROOT/config/default/eopf.toml"],
    secret=["$EOPF_CONFIG/secrets.json"],
    eoqc=EOQCConfig(
        config_folder="$EOPF_ROOT/qualitycontrol/config",
        parameters={},
        update_attrs=True,
        report_path="./reports/",
        config_path="$EOPF_ROOT/qualitycontrol/config/DUMMY_checklist.json",
        additional_config_folders=["./config"]
    ),
    environment={"DASK_ROOT_CONFIG": "~/.eopf/dask_config"}
)

# Dump to final payload file

output_file = Path("payload_example.yaml")
with output_file.open("w", encoding="utf-8") as f:
    f.write("# Triggering payload \n")
    
    yaml.dump(payload.model_dump(by_alias=True), f, sort_keys=False)

print(f"Payload YAML written to {output_file}")
