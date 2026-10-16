# rs-client-libraries

The Python library of [RS-Python](https://home.rs-python.eu/), the Copernicus Reference System Python: clients of the
RS-Python services and [Prefect](https://www.prefect.io/) flows to search, stage and process Copernicus Sentinel
products.

## Overview

| Package | Content |
|---|---|
| `rs_client` | Clients of the RS-Python services: [STAC](https://stacspec.org/) access to the external data sources (CADIP, AUXIP, PRIP), to the catalog, to the [CDSE](https://dataspace.copernicus.eu/) and to the [EarthDataHub](https://earthdatahub.destine.eu/); [OGC API - Processes](https://ogcapi.ogc.org/processes/) clients of the staging and DPR processing services; object storage credentials. |
| `rs_workflows` | The Prefect flows delivered with RS-Python: search, staging, conversion, generic DPR processing, Sentinel-1 and Sentinel-3 processing chains, operation flows. |
| `rs_common` | Common utilities: logging, [OpenTelemetry](https://opentelemetry.io/), Prefect blocks, S3 access, model classes and enums. |

## Getting started

The library is already installed in the JupyterHub of the RS-Python platforms. To install it elsewhere:

```shell
pip install rs-client-libraries
opentelemetry-bootstrap -a install
```

```python
import os
from rs_client.rs_client import RsClient

rs_client = RsClient(os.environ["RSPY_WEBSITE"], os.environ["RSPY_APIKEY"])
sessions = rs_client.get_cadip_client().search(collections=["sgs"], limit=10)
```

To develop on the library:

```shell
poetry install --with dev
poetry run opentelemetry-bootstrap -a install
poetry run pre-commit install
poetry run pytest -m unit
```

## Documentation

- [Overview and rs_client architecture](docs/doc/index.md)
- Prefect flows: [overview](docs/doc/flows/index.md), [authentication](docs/doc/flows/authentication.md),
  [task tables and payload file](docs/doc/flows/tasktable-payload.md), [common flows](docs/doc/flows/common-flows.md),
  [Level-0](docs/doc/flows/level0-flows.md), [Sentinel-1](docs/doc/flows/sentinel1-flows.md),
  [Sentinel-2](docs/doc/flows/sentinel2-flows.md) and [Sentinel-3](docs/doc/flows/sentinel3-flows.md) flows
- Advanced flow management: [customise the DPR processing](docs/doc/advanced/customise-dpr-flow.md),
  [systematic processing](docs/doc/advanced/systematic-processing.md), [create a custom flow](docs/doc/advanced/custom-flow.md)
- Operation: [object storage quota monitoring](docs/doc/operation/quota-monitoring.md),
  [performance indicators](docs/doc/operation/performance-indicators.md)
- Online documentation of the whole project: <https://home.rs-python.eu/rs-documentation/>
- Python package: <https://pypi.org/project/rs-client-libraries/>

## Related repositories

| Repository | Relationship |
|---|---|
| [rs-server](https://github.com/RS-PYTHON/rs-server) | The services called by `rs_client`. |
| [rs-dpr-service](https://github.com/RS-PYTHON/rs-dpr-service) | The DPR processing service called by the processing flows, and the task tables of the processors. |
| [rs-workflow-deployment](https://github.com/RS-PYTHON/rs-workflow-deployment) | Deployment of the flows and automations in Prefect. |
| [rs-workflow-env](https://github.com/RS-PYTHON/rs-workflow-env) | JupyterHub image with the library, Prefect workers, Dask clusters. |
| [rs-demo](https://github.com/RS-PYTHON/rs-demo) | Demo notebooks using the library. |
| [rs-documentation](https://github.com/RS-PYTHON/rs-documentation) | Online documentation of the whole project. |

## Contributing

See the [contribution guidelines](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/developers/contributing.md).
Report bugs in the [issue tracker](https://github.com/RS-PYTHON/rs-client-libraries/issues). In case of sensitive bugs
like security vulnerabilities, please contact <ops_coprs@airbus.com> directly instead of using the issue tracker.

## Licensing

The code in this project is licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

---

![](https://raw.githubusercontent.com/RS-PYTHON/.github/refs/heads/main/profile/banner_logo.jpg)

This project is funded by the EU and ESA.
