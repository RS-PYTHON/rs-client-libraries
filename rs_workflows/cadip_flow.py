# Copyright 2025 CS Group
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

"""CadipFlow implementation"""

from dataclasses import dataclass

from prefect import flow, get_run_logger, task


class CadipFlow:
    """Cadip prefect flows and tasks."""

    def __init__(
        self,
        cadip_collection_identifier: str,
        session_identifier: str,
        catalog_collection_identifier: str,
    ):
        self.cadip_col = cadip_collection_identifier
        self.session = session_identifier
        self.catalog_col = catalog_collection_identifier

    @task
    def myrun(self):
        logger = get_run_logger()

        logger.critical(self.cadip_col)
        logger.critical(self.session)
        logger.critical(self.catalog_col)
