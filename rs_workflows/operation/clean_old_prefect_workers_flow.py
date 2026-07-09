# Copyright 2023-2026 Airbus, CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta, timezone

import requests
from prefect import flow, get_run_logger
from prefect.settings import PREFECT_API_URL


@flow(name="cleanup-offline-workers")
def cleanup_offline_workers(
    work_pools: list[str],
    max_age_days: int = 30,
):
    """
    Delete workers that:
      - belong to one of the specified work pools
      - are currently OFFLINE
      - have not sent a heartbeat for more than max_age_days

    Parameters
    ----------
    work_pools : list[str]
        List of work pool names to inspect.

    max_age_days : int
        Minimum age (in days) since the last heartbeat before a worker
        becomes eligible for deletion.
    """

    logger = get_run_logger()

    # Read the Prefect API URL from the current Prefect configuration.
    # Example:
    #   http://prefect-server:4200/api
    api_url = str(PREFECT_API_URL.value())

    # Compute the oldest acceptable heartbeat timestamp.
    # Workers with a heartbeat newer than this date will be preserved.
    limit_date = datetime.now(timezone.utc) - timedelta(
        days=max_age_days
    )

    total_deleted = 0

    # Process every requested work pool
    for pool_name in work_pools:

        logger.info(f"Processing work pool '{pool_name}'")

        # Retrieve all workers registered in the work pool
        response = requests.get(
            f"{api_url}/work_pools/{pool_name}/workers",
            timeout=30,
        )

        response.raise_for_status()

        workers = response.json()

        # Inspect every worker
        for worker in workers:

            worker_name = worker["name"]

            # Only consider offline workers
            if worker["status"] != "OFFLINE":
                continue

            # Retrieve the last heartbeat timestamp, if available
            last_heartbeat = worker.get("last_heartbeat_time")

            if last_heartbeat:

                heartbeat_date = datetime.fromisoformat(
                    last_heartbeat.replace("Z", "+00:00")
                )

                # Keep workers that have gone offline recently
                if heartbeat_date > limit_date:
                    logger.info(
                        f"Skipping worker '{worker_name}' "
                        f"(last heartbeat: {last_heartbeat})"
                    )
                    continue

            logger.info(
                f"Deleting worker '{worker_name}' "
                f"from work pool '{pool_name}'"
            )

            # Delete the worker entry from the work pool
            delete_response = requests.delete(
                f"{api_url}/work_pools/{pool_name}/workers/{worker_name}",
                timeout=30,
            )

            delete_response.raise_for_status()

            total_deleted += 1

    logger.info(
        f"Cleanup completed: {total_deleted} worker(s) deleted."
    )


if __name__ == "__main__":
    cleanup_offline_workers(
        work_pools=[
            "kubernetes",
            "docker",
        ],
        max_age_days=30,
    )
