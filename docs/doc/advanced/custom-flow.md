# Create a custom flow

This page explains how to use the `rs_client` library and how to write, test and deploy your own Prefect flows.
It assumes that you know the basics of [Prefect](https://docs.prefect.io/v3/how-to-guides/workflows/write-and-run)
and that your [environment is set up](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/getting-started/setup-environment.md).

## Working environment

- **JupyterHub** (recommended to start): the libraries and the environment variables are already set up. Try your
  code first in a notebook, then move it to a Python module to make a flow of it. The
  [rs-demo](https://github.com/RS-PYTHON/rs-demo) repository contains tutorials, e.g.
  `notebooks/tutorials/prefect+dask/prefect.ipynb` that shows the different ways to run and deploy a flow.
- **Your IDE** (e.g. [Visual Studio Code](https://code.visualstudio.com/)): see the
  [coding environment](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/developers/coding-environment.md)
  and the [local mode](https://github.com/RS-PYTHON/rs-demo/blob/develop/docs/local-mode.md), that allows to debug
  your flows with breakpoints.

## Use the rs_client library

### Initialise the clients

```python
import os
from rs_client.rs_client import RsClient

# With your API key (recommended, mandatory for the flows)
rs_client = RsClient(os.environ["RSPY_WEBSITE"], rs_server_api_key=os.environ["RSPY_APIKEY"])

# Or, from a JupyterHub notebook only, with your OAuth2 session cookie read from the RSPY_OAUTH2_COOKIE environment
# variable. It has a short lifespan, so don't use it for long processing.
rs_client = RsClient(os.environ["RSPY_WEBSITE"])

auxip_client = rs_client.get_auxip_client()
cadip_client = rs_client.get_cadip_client()
prip_client = rs_client.get_prip_client()
catalog_client = rs_client.get_catalog_client()
staging_client = rs_client.get_staging_client()
dpr_client = rs_client.get_dpr_client()
osam_client = rs_client.get_osam_client()
```

### Browse the products

The STAC clients (`AuxipClient`, `CadipClient`, `PripClient`, `CatalogClient`, `CdseClient`, `EarthDataHubClient`)
share the same methods, based on [pystac-client](https://pystac-client.readthedocs.io/):

```python
collections = list(auxip_client.get_collections())
collection = auxip_client.get_collection("adgs")
queryables = auxip_client.get_queryables()

# Search with a CQL2 filter (JSON or text), a time interval, sorting and paging
items = auxip_client.search(
    collections=["adgs"],
    stac_filter={"op": "=", "args": [{"property": "product:type"}, "AUX_PP1"]},
    timestamp="2026-08-01T00:00:00Z/2026-08-02T00:00:00Z",
    sortby="-created",
    limit=10,
)
for item in items:
    print(item.id, item.properties["start_datetime"], list(item.assets))
```

The search parameters and the CQL2 filters are described in
[Browse products](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/services/browse-products.md).

### Manage your catalog collections

```python
from datetime import datetime
from pystac import Collection, Extent, SpatialExtent, TemporalExtent

collection = Collection(
    id="my-collection",
    description="My collection",
    extent=Extent(SpatialExtent([[-180, -90, 180, 90]]), TemporalExtent([[datetime(2026, 1, 1), None]])),
)
catalog_client.add_collection(collection)          # created as "<your login>:my-collection"
items = list(catalog_client.get_items("my-collection"))
catalog_client.remove_collection("my-collection")  # also removes its items
```

`add_item`, `update_item`, `patch_item` and `remove_item` manage the items. The assets of an item must be written
in the object storage before the item is added, see [Catalog](https://github.com/RS-PYTHON/rs-server/blob/develop/docs/doc/users/catalog.md).

### Stage products

```python
jobs = staging_client.run_staging(items.to_dict(), "my-collection")  # one job per station
final_status = staging_client.wait_for_jobs(jobs)
```

### Follow the DPR jobs

The processing is usually run by the `process-generic` flow (see
[Customise the DPR processing](customise-dpr-flow.md)). With the `DprClient`, you can follow or cancel the jobs:

```python
jobs = dpr_client.get_jobs()
job = dpr_client.get_job_info("<job id>")
dpr_client.delete_job("<job id>")
```

### Get your object storage credentials

```python
credentials = await osam_client.get_credentials()
print(credentials.endpoint, credentials.region, credentials.access_key)
```

## Write a flow

A flow is a Python function decorated with `@flow`. The RS-Python flows take an `env` parameter and create a
`FlowEnv` to get an authenticated `RsClient` instance and OpenTelemetry spans (see
[Authentication](../flows/authentication.md)):

```python
# my_flows.py
from prefect import flow, task
from pystac import ItemCollection

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.utils.prefect import get_logger


@task(name="count-items")
def count_items(items: ItemCollection) -> int:
    return len(items)


@flow(name="search-my-collection")
async def search_my_collection(env: FlowEnvArgs, collection: str) -> int:
    """Search all the items of one of my catalog collections and count them."""
    logger = get_logger()
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "search-my-collection"):
        catalog_client = flow_env.rs_client.get_catalog_client()
        items = catalog_client.search(collections=[collection], limit=100)
        count = count_items(items)
        logger.info(f"{count} items found in {collection!r}")
        return count
```

Reuse the existing flows as sub-flows or tasks rather than re-implementing them, e.g. the
[search and staging flows](../flows/common-flows.md) or `process-generic` with `call_dpr_flow` (see
[Customise the DPR processing](customise-dpr-flow.md#call-the-generic-flow-from-your-own-flow)).

## Run and deploy your flow

1. **Run it directly**, as a Python function, from a notebook or a script. The flow and its tasks run in your own
   environment (your JupyterHub session), and the run is visible in the Prefect dashboard. This is the fastest way
   to test your code:

    ```python
    from rs_workflows.flow_utils import FlowEnvArgs
    import my_flows

    await my_flows.search_my_collection(FlowEnvArgs(owner_id="<your login>"), "my-collection")
    ```

2. **Deploy it** to run it on the cluster, from the dashboard or from automations. Use the
   `PREFECT_WORK_POOL_SANDBOX` work pool for your own flows. The deployment tells the workers where to get your
   code, e.g. from a git repository, with a [prefect.yaml](https://docs.prefect.io/v3/how-to-guides/deployments/prefect-yaml) file:

    ```yaml
    prefect-version: "3"
    pull:
    - prefect.deployments.steps.git_clone:
        repository: https://github.com/<your organisation>/<your repository>.git
        branch: main
    deployments:
    - name: Search My Collection
      tags: ["searching", "custom"]
      entrypoint: ./my_flows.py:search_my_collection
      work_pool:
        name: "{{ PREFECT_WORK_POOL_SANDBOX }}"
    ```

    ```shell
    prefect deploy --all --prefect-file ./prefect.yaml
    ```

    While developing, you can also upload your code to an S3 bucket instead of a git repository, so you don't have
    to push each modification: see the `deploy-yaml-s3.yaml` example of the rs-demo tutorials.

3. **Share it**: push your flow to your own git repository, or propose it to RS-Python with a pull request to
   [rs-client-libraries](https://github.com/RS-PYTHON/rs-client-libraries) (see
   [Contributing](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/developers/contributing.md)).
   The flows of rs-client-libraries are deployed on all the platforms by
   [rs-workflow-deployment](https://github.com/RS-PYTHON/rs-workflow-deployment/blob/develop/README.md).

Follow the [naming conventions](../flows/index.md#naming-conventions) for your flows, deployments and tags.
