# Level-0 flows

The Level-0 flows generate the Level-0 products from the raw data sessions of the CADIP stations, with the DPR
Level-0 processors.

| Flow | Parameters | Description |
|---|---|---|
| `process-l0` | `session`, `env_args` | Generic entry point for all the missions, started by the systematic processing after the staging of a session: checks the session, stages it if needed, then runs the Level-0 flow of the mission (`process-s1-l0` or `process-s3-l0`) as a sub-flow. |
| `process-s1-l0` | `session`, `env_args` | Sentinel-1 Level-0 processing of a session already staged, see [Sentinel-1 flows](sentinel1-flows.md). |
| `process-s3-l0` | `session`, `env_args` | Sentinel-3 Level-0 processing of a session already staged, see [Sentinel-3 flows](sentinel3-flows.md). |

The parameters of a run are the `session`, e.g. `S3A_20260807020505053186`, and the owner of the products
(`env_args`, whose `process` field also sets `force_systematic` and `verbose`). The other parameters are read from the default settings of the Level-0 processing of the mission, in
the `s1l0-default-setting` or `s3l0-default-setting` Prefect variable (see
[Customise the DPR processing](../advanced/customise-dpr-flow.md#default-settings-of-the-processing-flows)).
With `env_args.process.force_systematic`, the workflow of the processing is `systematic`: the events of the generated products start
the next processing of the chain. In systematic processing, `process-l0` is started with `process.force_systematic` by an
automation after the staging of each session, see [Systematic processing](../advanced/systematic-processing.md).

## Steps of `process-l0`

1. Check the session identifier and deduce the mission from it.
2. Read the default settings of the Level-0 processing of the mission.
3. Check that the Dask cluster given by `dask_cluster_name` and the `dask-staging` cluster are running.
4. Look for the session in the catalog collections `input_collections`. If it is not there, find it in the CADIP
   collections `cadip_collections` and stage it. An unpublished (expired) session is rejected.
5. Run the Level-0 flow of the mission (`process-s1-l0` or `process-s3-l0`) as a sub-flow, that reads the settings
   again and runs the generic processing flow `process-generic` with:
    - the session as input product,
    - the session date as `start_datetime` and `end_datetime` (the sensing dates are not available for a session),
    - the satellite deduced from the session identifier,
    - the processor, pipeline, Dask cluster and collections of the default settings.

The generated products are published in the catalog collections given by
`generated_product_to_collection_identifier`.
