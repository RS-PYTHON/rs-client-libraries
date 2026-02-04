#### From OVH administration dashboard

1. Create bucket: **```<PLATFORM>```-access-logs**  
   For operation, we use **rspython-ops** for **```<PLATFORM>```**
   <br>
2. Enable server access logging following the procedure:  
   https://help.ovhcloud.com/csm/fr-public-cloud-storage-s3-server-access-logging?id=kb_article_view&sysparm_article=KB0056623  
   
   Use the following configuration:

   * The bucket chosen to store the log must be: **```<PLATFORM>```-access-logs**. 
   * For each bucket that must be monitored (i.e., buckets hosting end‑user products), apply:

```json
   {
     "LoggingEnabled": {
       "TargetBucket": "<PLATFORM>-access-logs",
       "TargetPrefix": "prip/"
     }
   }
```
<br>

3. Create an OBS account with the description: **"Quota monitoring : access to the bucket \<PLATFORM>-access-logs**
<br>

4. Grant admin rights to this OBS account on the bucket: **```<PLATFORM>```-access-logs**
<br>
<br>

#### From Keycloak
1. Create the account **operator-quota** and assign tge role **RS-JUPYTER-USER**
<br><br>

#### From Jupyter Hub
1. Log in using the account **operator-quota**
    <br>
2. Generate the **env-var-operator-quota** Prefect Block with following additional fields.

To access to the database **s3_quota**:
- POSTGRES_QUOTA_USER"
- POSTGRES_QUOTA_PASSWORD

Access to bucket **```<PLATFORM>```-access-logs**:
- S3_QUOTA_REGION
- S3_QUOTA_ENDPOINT
- S3_QUOTA_ACCESSKEY
- S3_QUOTA_SECRETKEY


This procedure will do the job.

```python
from rs_common.prefect_utils import update_prefect_block
import getpass

BLOCK_NAME_ENV_USER: str = "env-vars-{0}"
owner_id = os.getenv("JUPYTERHUB_USER", "")
env_vars = {}

password = getpass.getpass("Password for POSTGRES_QUOTA_USER: ")
access_key = getpass.getpass("Access key to access the bucket: <PLATFORM>-access-logs")
secret_key = getpass.getpass("Secret to access the bucket: <PLATFORM>-access-logs")

env_vars.update(
    {
        "POSTGRES_QUOTA_USER": "quotastestuserdb",
        "POSTGRES_QUOTA_PASSWORD": password,
        "S3_QUOTA_REGION" :os.environ["S3_REGION"],
        "S3_QUOTA_ENDPOINT" : os.environ["S3_ENDPOINT"],
        "S3_QUOTA_ACCESSKEY" : access_key,
        "S3_QUOTA_SECRETKEY" : secret_key
    })

await update_prefect_block(format_env_user(BLOCK_NAME_ENV_USER, owner_id), env_vars) 
```

<br>


#### From Prefect
1. Run the flow **collect-obs-logs** periodically with owner id **operator-quota**
<br>

#### From Grafana
1. Build a dashboard showing READ, WRITE, and DOWNLOAD operations per user and per bucket over time.
- The database is named : **s3_quotas**
- The table to be read is : **s3_access_log**


