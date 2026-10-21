# Authentication

The flows run on the Prefect workers of the cluster, on behalf of a user given by the `owner_id` field of their `env`
parameter. They authenticate to the RS-Python services and to the object storage with the information stored in
[Prefect secret blocks](https://docs.prefect.io/v3/concepts/blocks).

## Authentication to the services

The flows **always use the API key** of the user to call the services, never the OAuth2 session cookie, because the
cookie has a short lifespan and the flows may run for hours. See
[User rights and API keys](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/getting-started/user-rights.md)
for the creation of the API keys and the available rights.

At startup, each flow creates a `FlowEnv` object from its `env` parameter, that:

1. reads the environment variables shared by all the users from the `env-vars` Prefect block (created by an
   administrator, see [Initialise the processing environment](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/administration/init-processing-environment.md)),
2. reads the environment variables of the user from the `env-vars-<owner_id>` Prefect block: `RSPY_APIKEY` and the
   object storage credentials (created by the user, see
   [Set up your environment](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/getting-started/setup-environment.md)),
3. creates an `RsClient` instance (`flow_env.rs_client`) with the URL of the services (`RSPY_WEBSITE`) and the API
   key of the user, used to call all the services,
4. initialises the OpenTelemetry traces.

```python
from prefect import flow
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs

@flow(name="my-flow")
async def my_flow(env: FlowEnvArgs):
    flow_env = FlowEnv(env)  # read the Prefect blocks of env.owner_id
    with flow_env.start_span(__name__, "my-flow"):
        catalog_client = flow_env.rs_client.get_catalog_client()
        ...
```

The flow runs with the rights of the API key owner: if the owner has no right on a station or a collection, the
flow fails with a `401` error.

## Authentication to the object storage

The flows and the processors read and write the products directly in the S3 object storage, with the credentials of
the user returned by [OSAM](https://github.com/RS-PYTHON/rs-server/blob/develop/docs/doc/users/osam.md) and saved in
the `S3_ACCESSKEY`, `S3_SECRETKEY`, `S3_ENDPOINT` and `S3_REGION` variables of the `env-vars-<owner_id>` block. Other
object storages can be configured with `S3_<ID>_ACCESSKEY`, `S3_<ID>_SECRETKEY`, `S3_<ID>_ENDPOINT` and
`S3_<ID>_REGION` variables, e.g. to read a digital elevation model from another provider (see the
[storage configuration](../advanced/customise-dpr-flow.md#storage-configuration)):

```python
from rs_client.osam_client import BucketCredentials
from rs_common.prefect_utils import save_bucket_credentials

# Save the credentials of the user from OSAM, and of another object storage with the "DEMS1" identifier
await save_bucket_credentials(
    osam_client=rs_client.get_osam_client(),
    obs_credentials={"DEMS1": BucketCredentials(access_key="...", secret_key="...", endpoint="...", region="...")},
)
```

Then the S3 helpers of `rs_common.prefect_utils` (`s3_download_file`, `s3_upload_file`, `s3_upload_dir`...) can be
used to access the object storage, e.g. to download the products, see
[Browse products](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/services/browse-products.md#download-the-assets).

## Authentication to the Dask clusters

The processing flows and services connect to the Dask clusters through Dask Gateway, with the JupyterHub API token
(`JUPYTERHUB_API_TOKEN` variable of the `env-vars` block). This is transparent for the users.
