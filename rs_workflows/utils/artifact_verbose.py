# Copyright 2026 Airbus defence And Space
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

""" Report Manager to create artifact table """

from datetime import datetime

from prefect.artifacts import acreate_table_artifact  # type: ignore # pylint: disable=import-error


class ReportManager:
    """
    Manages execution reports for a multi-step flow and publishes them
    as Prefect artifacts.

    Attributes
    ----------
    number_steps : int
        Total number of steps expected in the flow.
    report : list[dict]
        Accumulated list of step results.
    """

    def __init__(self):
        self.report = []

    def success_step(self, step: int, description: str) -> None:
        """
        Register a successful step in the report.
        """
        item = {
            "step": step,
            "description": description,
            "status": "OK",
        }
        self.report.append(item)

    def failed_step(self, step: int, description: str) -> None:
        """
        Register a failed step in the report.
        """
        item = {
            "step": step,
            "description": description,
            "status": "NOK",
        }
        self.report.append(item)

    async def push_report(self, key_value: str, description_value: str):
        """
        Publish the accumulated report as a Prefect table artifact.

        Notes
        -----
        - Prefect artifact keys must contain only lowercase letters,
          numbers, and dashes.
        - A timestamp is appended to the description for traceability.

        Returns
        -------
        ArtifactResponse
            The Prefect artifact object returned by create_table_artifact().
        """
        now = datetime.now()
        timestamp = now.strftime("%A %d %B %Y, %H:%M:%S")

        await acreate_table_artifact(
            key=key_value.lower(),
            table=self.report,
            description=f"{description_value} - {timestamp}",
        )
