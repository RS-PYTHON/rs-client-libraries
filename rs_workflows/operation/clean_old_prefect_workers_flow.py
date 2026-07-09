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
from urllib.parse import quote

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

    logger.info(
        f"work_pools parameter = {work_pools!r}"
    )

    # Read the Prefect API URL from the current Prefect configuration.
    # Example:
    #   http://prefect-server:4200/api
    api_url = str(PREFECT_API_URL.value()).rstrip("/")

    # Prefect API maximum accepted limit for this endpoint.
    page_size = 200

    # Compute the oldest acceptable heartbeat timestamp.
    # Workers with a heartbeat newer than this date will be preserved.
    limit_date = datetime.now(timezone.utc) - timedelta(
        days=max_age_days
    )

    total_deleted = 0

    # Process every requested work pool.
    for pool_name in work_pools:

        logger.info(f"Processing work pool '{pool_name}'")

        offset = 0
        workers_to_delete = []

        # Retrieve workers page by page.
        while True:
            response = requests.post(
                f"{api_url}/work_pools/{pool_name}/workers/filter",
                json={
                    "offset": offset,
                    "limit": page_size,
                },
                timeout=30,
            )

            if response.status_code == 404:
                logger.warning(
                    f"Work pool '{pool_name}' does not exist. Skipping."
                )
                break

            if not response.ok:
                logger.error(
                    f"Failed to list workers from work pool '{pool_name}' "
                    f"(offset={offset}, limit={page_size}): "
                    f"{response.status_code} - {response.text}"
                )
                response.raise_for_status()

            workers = response.json()

            logger.info(
                f"Retrieved {len(workers)} worker(s) "
                f"from work pool '{pool_name}' "
                f"(offset={offset}, limit={page_size})"
            )

            # Inspect every worker from the current page.
            for worker in workers:

                worker_name = worker["name"]
                worker_status = worker["status"]

                # Only consider offline workers.
                if worker_status != "OFFLINE":
                    continue

                # Retrieve the last heartbeat timestamp, if available.
                last_heartbeat = worker.get("last_heartbeat_time")

                if last_heartbeat:
                    heartbeat_date = datetime.fromisoformat(
                        last_heartbeat.replace("Z", "+00:00")
                    )

                    # Keep workers that have gone offline recently.
                    if heartbeat_date > limit_date:
                        logger.info(
                            f"Skipping worker '{worker_name}' "
                            f"from work pool '{pool_name}' "
                            f"(status={worker_status}, "
                            f"last_heartbeat={last_heartbeat})"
                        )
                        continue

                # Collect workers first, delete them later.
                # This avoids modifying the result set while paginating.
                workers_to_delete.append(worker)

            # Stop when the current page contains fewer items than the page size.
            if len(workers) < page_size:
                break

            offset += page_size

        logger.info(
            f"Found {len(workers_to_delete)} worker(s) eligible for deletion "
            f"from work pool '{pool_name}'"
        )

        # Delete collected workers after pagination is complete.
        for worker in workers_to_delete:
            worker_name = worker["name"]
            worker_status = worker["status"]
            last_heartbeat = worker.get("last_heartbeat_time")

            logger.info(
                f"Deleting worker '{worker_name}' "
                f"from work pool '{pool_name}' "
                f"(status={worker_status}, "
                f"last_heartbeat={last_heartbeat})"
            )

            # Worker names may contain spaces, so the name must be URL-encoded.
            encoded_worker_name = quote(worker_name, safe="")

            delete_response = requests.delete(
                f"{api_url}/work_pools/{pool_name}/workers/{encoded_worker_name}",
                timeout=30,
            )

            if not delete_response.ok:
                logger.error(
                    f"Failed to delete worker '{worker_name}' "
                    f"from work pool '{pool_name}': "
                    f"{delete_response.status_code} - {delete_response.text}"
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
