# Sentinel-3 flows

The [Sentinel-3](https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-3) processing chain available in
RS-Python goes from the raw data sessions to the OLCI Level-2 products and their quicklooks:

```mermaid
flowchart LR
    cadip[("CADIP<br/>session")]
    stage["stage-cadip-with-options<br/>(Stage Cadip)"]
    l0["process-s3-l0"]
    l1["process-s3-l1-olci"]
    l2["process-s3-l2-olci"]
    ql1["generate-s3-l1-olci-quicklooks"]
    ql2["generate-s3-l2-olci-quicklooks"]
    cadip --> stage --> l0 --> l1 --> l2
    l1 --> ql1
    l2 --> ql2
```

| Flow | Parameters | Description | Processor |
|---|---|---|---|
| `process-s3-l0` | `session`, `flow_params`, `verbose` | Level-0 processing of a session already staged (see also `process-l0` in [Level-0 flows](level0-flows.md)). Emits the `rs-python.s3-l0.products-ready` event. | `s3_l0` |
| `process-s3-l1-olci` | `flow_params`, `l0_products`, `source_l0_run_id` | OLCI Level-1 processing. The input products are either given in `flow_params.input_products`, or built from the `l0_products` list sent by the Level-0 event (3 `S03OLCL0_` products and 1 `S03NATL0_` product). Emits the `rs-python.s3-l1.products-ready` and `rs-python.s3-l1.quicklook-inputs-ready` events. | `s3_l1olci` |
| `process-s3-l2-olci` | `flow_params`, `input_products` | OLCI Level-2 processing of the `S03OLCEFR` Level-1 products. Emits the `rs-python.s3-l2.quicklook-inputs-ready` event. | `s3_l2olci` |
| `generate-s3-l1-olci-quicklooks` | `owner_id`, `published_items` | Generate an RGB quicklook of each Level-1 product, as a JPEG image and a georeferenced [Cloud Optimized GeoTIFF](https://cogeo.org/), upload them next to the product and add them as `thumbnail` assets of its STAC item. | |
| `generate-s3-l2-olci-quicklooks` | `owner_id`, `published_items` | Same for the Level-2 products. | |

The flows are chained automatically by Prefect automations, see
[Systematic processing](../advanced/systematic-processing.md). They can also be run individually from the Prefect
dashboard.

The default settings of the Sentinel-3 flows are read from the `s3-processing-default-setting` Prefect variable
(`l0`, `l1` and `l2` sections, plus the `orchestration` section used by the automations), see
[Customise the DPR processing](../advanced/customise-dpr-flow.md#default-settings-of-the-processing-flows).

The task tables of the Sentinel-3 processors are in the
[rs-dpr-service config](https://github.com/RS-PYTHON/rs-dpr-service/tree/develop/config) folder
(`TaskTable_S3_L0_*.json`, `TaskTable_S3_L1OLCI_*.json`, `TaskTable_S3_L2OLCI_*.json`, `TaskTable_S3_L1SLSTR_*.json`).
