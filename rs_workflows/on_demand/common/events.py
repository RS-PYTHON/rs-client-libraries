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

"""Common event naming for on-demand processing chains."""

PRODUCTS_READY_EVENT = "rs-python.{mission}-{level}.products-ready"

#s3l0, s3L1

def products_ready_event_name(mission: str, level: str) -> str:
    """Return the generic products-ready event name for a processing step."""
    mission_name = mission if mission.startswith("s") else f"s{mission}"
    level_name = level if level.startswith("l") else f"l{level}"
    return PRODUCTS_READY_EVENT.format(mission=mission_name, level=level_name)
