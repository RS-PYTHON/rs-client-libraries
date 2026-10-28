# Sentinel-1 flows

The [Sentinel-1](https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-1) processing available in
RS-Python is:

| Processing | How to run it | Processor (rs-dpr-service process) |
|---|---|---|
| Level-0 | `process-l0` or `process-s1-l0` flows, see [Level-0 flows](level0-flows.md). The session is the only input product (`S1CADUS`). In systematic processing, it is started automatically after the staging of the session, see [Systematic processing](../advanced/systematic-processing.md). | `s1_l0` |
| Analysis Ready Data (ARD) | `process-s1-ard-reference` then `process-s1-ard` flows, see [Analysis Ready Data](#analysis-ready-data-ard). They can also be run with the `process-generic` flow, with the `s1_ard` processor and one of the pipelines of its task table, see [Customise the DPR processing](../advanced/customise-dpr-flow.md). | `s1_ard` |

The default settings of the Sentinel-1 flows are read from the `s1l0-default-setting` (Level-0) and
`s1ard-default-setting` (ARD) Prefect variables, see
[Customise the DPR processing](../advanced/customise-dpr-flow.md#default-settings-of-the-processing-flows).

The task tables of these processors, with their pipelines, inputs and auxiliary files, are in the
[rs-dpr-service config](https://github.com/RS-PYTHON/rs-dpr-service/tree/develop/config) folder
(`TaskTable_S1_L0_*.json`, `TaskTable_S1_ARD_*.json`). The Sentinel-1 ARD processor also needs a digital elevation
model, read from the [EarthDataHub](https://earthdatahub.destine.eu/) of [Destination Earth](https://destination-earth.eu/)
(`edh_api_key` parameter).

## Analysis Ready Data (ARD)

The ARD processing generates products from Sentinel-1 SLC products (`IW`, `EW` or `SM` instrument mode), already
converted to Zarr and published in the catalog. The pipelines of the `s1_ard` task table are:

| Pipeline | Description | Output |
|---|---|---|
| `ARD_REFERENCE_PIPELINE` | Reference pipeline: computes the reference geometry of a master SLC and writes it in the `REFERENCE_DB`. | `REFERENCE_DB` |
| `SLC__1_CSL__1` | Co-registered SLC. | `cslcs` |
| `SLC__1_GSL__1` | Geocoded Single Look Complex. | `gslcs` |
| `SLC__1_NRB__1` | Normalised Radar Backscatter (mosaicked on the Sentinel-2 tiles, `S2_TILES` auxiliary file). | `nrb` |

These pipelines are also available in the `DprPipeline` enum of `rs_client` (`S1ARD_REF`, `S1ARD_CSL`,
`S1ARD_GSL`, `S1ARD_NRB`).

The flows are:

| Flow | Parameters | Description |
|---|---|---|
| `process-s1-ard-reference` | `slc`, `edh_api_key`, `env_args`, `verbose` | Run once on a master SLC to generate the `REFERENCE_DB` used by the nominal flow, with the `ARD_REFERENCE_PIPELINE` pipeline. |
| `process-s1-ard` | `slcs`, `reference_date`, `edh_api_key`, `env_args`, `pipeline`, `verbose` | Nominal on-demand ARD processing of several SLCs, `reference_date` being the date of the master SLC, with the given `pipeline` (`SLC__1_CSL__1`, `SLC__1_GSL__1` or `SLC__1_NRB__1`), or by default the pipeline of the settings. |

- `slc` and `slcs` are STAC item identifiers of the SLC products, searched in the `input_collections` of the
  settings (default: `s01sewslc`, `s01siwslc`, `s01ssmslc`). All the SLCs must share the same
  `sar:instrument_mode`, which is given to the processor as the `instrument_mode` external variable.
- `reference_date` is required when several SLCs are given. For the `SLC__1_NRB__1` pipeline with a single SLC, it
  is deduced from the SLC identifier, to select the `S2_TILES` auxiliary file.
- The `ETAD` and `ORBIT` auxiliary files are staged once per input SLC (`folder_multiplicity: one_per_input` in the
  task table), see [Task table and payload](tasktable-payload.md#2-find-and-stage-the-auxiliary-files).

The `REFERENCE_DB` is handled specifically: it is never searched nor staged. For the `ARD_REFERENCE_PIPELINE`, the
flow allocates a new folder `s3://<bucket>/<owner>/s1-ard-reference-db/<uuid>` for it, the bucket being given by the
[bucket configuration](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/administration/update-rs-server.md#object-storage-buckets-and-data-lifecycle)
for the owner, the `s1-ard-reference-db` collection and the `ARD_REFERENCE_DB` product type. Its path is written in
the logs of the flow.
