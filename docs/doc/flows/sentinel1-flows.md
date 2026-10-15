# Sentinel-1 flows

The [Sentinel-1](https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-1) processing available in
RS-Python is:

| Processing | How to run it | Processor (rs-dpr-service process) |
|---|---|---|
| Level-0 | `process-l0` or `process-s1-l0` flows, see [Level-0 flows](level0-flows.md). The session is the only input product (`S1CADUS`). | `s1_l0` |
| Analysis Ready Data (ARD) | `process-generic` flow, with the `s1_ard` processor and one of the pipelines of its task table, see [Customise the DPR processing](../advanced/customise-dpr-flow.md). | `s1_ard` |

The default settings of the Sentinel-1 flows are read from the `s1-processing-default-setting` Prefect variable
(`l0` section for the Level-0), see
[Customise the DPR processing](../advanced/customise-dpr-flow.md#default-settings-of-the-processing-flows).

The task tables of these processors, with their pipelines, inputs and auxiliary files, are in the
[rs-dpr-service config](https://github.com/RS-PYTHON/rs-dpr-service/tree/develop/config) folder
(`TaskTable_S1_L0_*.json`, `TaskTable_S1_ARD_*.json`). The Sentinel-1 ARD processor also needs a digital elevation
model, read from the [EarthDataHub](https://earthdatahub.destine.eu/) of [Destination Earth](https://destination-earth.eu/)
(`edh_api_key` parameter of `process-generic`).
