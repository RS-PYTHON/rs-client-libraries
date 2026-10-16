# Performance indicators

The performance indicators (PI) measure the completeness and the timeliness of the production: are all the
expected products generated, and are they generated in time? They are computed by the processing flows and stored
in a PostgreSQL database (`performance` by default), displayed by Grafana with the `Postgres-performance` data
source.

## How they are computed

At the end of each DPR processing, the `process-generic` flow records:

1. the **flow run**: type (systematic, on-demand, benchmarking), mission, processor, input products, start and stop
   dates and status of the processing,
2. the **expected products**: for each product type, the minimum and maximum number of products that the processor
   should generate from its inputs, and their estimated sensing start. The estimation depends on the processor and
   on its inputs,
3. the **realised products**: the generated STAC items, with their sensing start, their origin date (`t0`, the
   availability of the input data, e.g. of the last chunk of the session at the CADIP station) and their storage
   date in the catalog (`tf`),
4. the **missing products**, by comparing the expected and realised products, and the products that were not
   expected,
5. the **timeliness** of each realised product: whether `tf - t0` is within the maximum delay of its PI category,
   or within this delay plus 1, 2, 3 or 7 days.

## Database model

The tables are created by the `initialize-pi-db` flow, that also fills the PI categories.

| Table | Content |
|---|---|
| `flow_run` | One line per processing: `flow_type`, `mission`, Prefect flow identifiers, Dask and Python versions, processor name and unit, input STAC items, processing start and stop dates, status, `excluded_from_pi` flag (e.g. to exclude a reprocessing from the indicators). |
| `pi_category` | The PI categories of each mission, with their maximum delay (`max_delay_seconds`). |
| `product_expected` | The expected products of a flow run: PI category, product type, estimated sensing start, `min_count`, `max_count`. |
| `product_realised` | The realised products of a flow run: PI category, product type, STAC item, sensing start, origin date, storage date, `unexpected` flag, `on_time_0_day`, `on_time_1_day`, `on_time_2_day`, `on_time_3_day`, `on_time_7_day` flags. |
| `product_missing` | The missing products of a flow run: PI category, product type, sensing start and count. |

The PI categories are defined in the [`init_pi_db_flow.py`](https://github.com/RS-PYTHON/rs-client-libraries/blob/develop/rs_workflows/init_pi_db_flow.py)
module, e.g. for Sentinel-3: `HKAN` (housekeeping and navigation data), `NRT` (near real time products), `STC1` and
`STC2` (short time critical products), `NTC1` and `NTC2` (non time critical products). Each product type is
associated to a category.

## Set up

1. Create the `performance` database and its user at deployment time (`rs_performance_indicator` inventory
   variables).
2. Set the `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_PI_DB` variables in
   the `env-vars` Prefect block (see [Initialise the processing environment](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/administration/init-processing-environment.md)).
3. Run the `initialize-pi-db` flow once.
4. Build the Grafana dashboards from the `Postgres-performance` data source.
