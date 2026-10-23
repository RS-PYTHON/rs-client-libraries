# Systematic processing

In **on-demand** processing, a user runs a flow with its parameters. In **systematic** processing, the flows are
started automatically when new data is available, so that the products are generated as soon as possible without
any human action. RS-Python implements the systematic processing with
[Prefect events and automations](https://docs.prefect.io/v3/concepts/automations): each processing flow emits an
event when its products are published, and an automation starts the next flow of the chain when it receives the
event.

The same flows are used for both kinds of processing: the systematic chain only adds the automations, deployed by
[rs-workflow-deployment](https://github.com/RS-PYTHON/rs-workflow-deployment/blob/develop/README.md). The
`workflow` parameter of the flows (`systematic` or `on-demand`) is recorded with the
[performance indicators](../operation/performance-indicators.md).

## Sentinel-3 chain

```mermaid
sequenceDiagram
    participant op as Operator or schedule
    participant stage as Stage Cadip<br/>(stage-cadip-with-options)
    participant l0 as Process S3 L0
    participant l1 as Process S3 L1 OLCI
    participant l2 as Process S3 L2 OLCI
    participant ql as Quicklook flows
    op->>stage: stage a session
    stage-->>l0: automation "CADIP staging completed -> S3 L0"<br/>(prefect.flow-run.Completed)
    l0-->>l1: automation "S3 L0 products ready -> S3 L1 OLCI"<br/>(rs-python.s3-l0.products-ready)
    l1-->>l2: automation "S3 L1 products ready -> S3 L2 OLCI"<br/>(rs-python.s3-l1.products-ready)
    l1-->>ql: automation "S3 L1 OLCI quicklook"<br/>(rs-python.s3-l1.quicklook-inputs-ready)
    l2-->>ql: automation "S3 L2 OLCI quicklook"<br/>(rs-python.s3-l2.quicklook-inputs-ready)
```

| Automation | Trigger | Action |
|---|---|---|
| CADIP staging completed -> S3 L0 | A flow run of the `Stage Cadip` deployment is completed. | Run `Process S3 L0` with the staged session. |
| S3 L0 products ready -> S3 L1 OLCI | Event `rs-python.s3-l0.products-ready`. | Run `Process S3 L1 OLCI` with the Level-0 products of the event. |
| S3 L1 products ready -> S3 L2 OLCI | Event `rs-python.s3-l1.products-ready`. | Run `Process S3 L2 OLCI` with the `S03OLCEFR` products of the event. |
| S3 L1 OLCI quicklook on quicklook-inputs-ready | Event `rs-python.s3-l1.quicklook-inputs-ready`. | Run `S3L1 OLCI quicklook` with the published items of the event. |
| S3 L2 OLCI quicklook on quicklook-inputs-ready | Event `rs-python.s3-l2.quicklook-inputs-ready`. | Run `S3L2 OLCI quicklook` with the published items of the event. |

The other parameters of the flows are read from their [default settings](customise-dpr-flow.md#default-settings-of-the-processing-flows).

## Events

The events are emitted with [`emit_event`](https://docs.prefect.io/v3/concepts/events) at the end of the flows.
Their name follows the `rs-python.<mission>-<level>.<what>` pattern, e.g. `rs-python.s3-l0.products-ready`
(`products_ready_event_name()` in `rs_workflows.on_demand.common.events`). Because the Prefect event payloads are
limited in size, they only contain the product types and identifiers, not the complete STAC items:

```json
{
  "event": "rs-python.s3-l0.products-ready",
  "resource": {
    "prefect.resource.id": "rs-python.s3-l0-result.<flow run id>",
    "prefect.resource.name": "S3A_20260807020505053186",
    "rs-python.session-id": "S3A_20260807020505053186"
  },
  "related": [{"prefect.resource.id": "prefect.flow-run.<flow run id>", "prefect.resource.role": "flow-run"}],
  "payload": {
    "flow_run_id": "<flow run id>",
    "session_id": "S3A_20260807020505053186",
    "products": [{"S03OLCL0_": "<item id>"}, {"S03NATL0_": "<item id>"}]
  }
}
```

The events and the automations runs are visible in the *Event feed* and *Automations* pages of the Prefect
dashboard.

## Product events of `process-any`

The flows based on `process-any` (see [Sentinel-3 flows](../flows/sentinel3-flows.md#flows-based-on-process-any))
are chained by product: at the end of a systematic run, `process-any` emits a `product.<product type>.generated`
event for each generated product, and an automation starts the systematic flow of the next processing for this
product.

```mermaid
flowchart LR
    any1["process-any<br/>(S3 L0)"]
    l1["process-s3-l1-olci-systematic"]
    any2["process-any<br/>(S3 L1 OLCI)"]
    l2["process-s3-l2-olci-systematic"]
    any3["process-any<br/>(S3 L2 OLCI)"]
    any1 -.->|"product.S03OLCL0_.generated"| l1 --> any2
    any2 -.->|"product.S03OLCEFR.generated"| l2 --> any3
```

| Automation | Trigger | Action |
|---|---|---|
| S03OLCL0_ generated -> S3 L1 OLCI | Event `product.S03OLCL0_.generated`. | Run `process-s3-l1-olci-systematic` with the payload of the event. |
| S03OLCEFR generated -> S3 L2 OLCI | Event `product.S03OLCEFR.generated`. | Run `process-s3-l2-olci-systematic` with the payload of the event. |

The event is related to the flow run that generated the product, and to the `processing` and platform (e.g.
`sentinel-3a`) tags. Its payload contains the owner and the STAC item of the product, used by the next flow to
compute its inputs:

```json
{
  "event": "product.S03OLCL0_.generated",
  "resource": {"prefect.resource.id": "prefect.flow-run.<flow run id>", "prefect.resource.name": "S03OLCL0_"},
  "payload": {
    "owner_id": "copernicus",
    "product": {
      "id": "<item id>",
      "geometry": {"type": "Polygon", "coordinates": ["..."]},
      "collection": "s03olcl0_",
      "properties": {"product:type": "S03OLCL0_", "platform": "sentinel-3a", "start_datetime": "...", "end_datetime": "..."}
    }
  }
}
```

An on-demand flow run with `force_systematic` also emits these events, to start the rest of the chain from a given
product.

## Add a step to the chain

1. Emit an event at the end of the upstream flow, with the information needed by the next one in its payload.
2. Write the downstream flow so that it accepts this information as parameters (see e.g. the `l0_products`
   parameter of `process-s3-l1-olci`).
3. Add an automation that matches the event and runs the deployment of the downstream flow, with its parameters
   rendered from the event with [Jinja templates](https://docs.prefect.io/v3/concepts/automations#templating-with-jinja),
   in [rs-workflow-deployment](https://github.com/RS-PYTHON/rs-workflow-deployment/blob/develop/README.md).
