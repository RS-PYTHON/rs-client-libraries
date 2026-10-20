# Level-0 flows

The Level-0 flows generate the Level-0 products from the raw data sessions of the CADIP stations, with the DPR
Level-0 processors.

| Flow | Parameters | Description |
|---|---|---|
| `process-l0` | `session`, `flow_params`, `verbose` | Generic entry point for all the missions: checks the session, stages it if needed, then calls the flow of the mission. |
| `process-s1-l0` | `session`, `flow_params`, `verbose` | Sentinel-1 Level-0 processing of a session already staged, see [Sentinel-1 flows](sentinel1-flows.md). |
| `process-s3-l0` | `session`, `flow_params`, `verbose` | Sentinel-3 Level-0 processing of a session already staged, see [Sentinel-3 flows](sentinel3-flows.md). |

Only the `session` parameter is mandatory, e.g. `S3A_20260807020505053186`: the other parameters (`flow_params`)
are read from the default settings of the mission, in the `s<mission>-processing-default-setting` Prefect variable,
and can be overridden for a given run (see
[Customise the DPR processing](../advanced/customise-dpr-flow.md#default-settings-of-the-processing-flows)).

## Steps of `process-l0`

1. Check the session identifier and deduce the mission from it.
2. Read the default settings of the mission and merge them with the `flow_params` parameter.
3. Check that the Dask cluster given by `dask_cluster_label` is running.
4. Look for the session in the catalog collection `session_collection`. If it is not there, find it in the CADIP
   collections `cadip_collections` and stage it. An unpublished (expired) session is rejected.
5. Call the flow of the mission, that calls the generic processing flow `process-generic` with:
    - the session as input product,
    - the session date as `start_datetime` and `end_datetime` (the sensing dates are not available for a session),
    - the satellite deduced from the session identifier,
    - the processor, pipeline, Dask cluster and collections of the default settings.

The generated products are published in the catalog collections given by
`generated_product_to_collection_identifier`.
