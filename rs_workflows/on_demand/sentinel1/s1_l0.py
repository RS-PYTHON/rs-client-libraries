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

"""sentinel 1 Level-0 processing."""

from typing import Any

from prefect import flow

from rs_workflows.flow_utils import FlowEnvArgs
from rs_workflows.on_demand.common.l0_last_steps import process_staged_session


@flow(name="process-s1-l0", flow_run_name="s1-l0-{session}")
async def process_s1l0(
    session: str,
    env_args: FlowEnvArgs,
) -> list[dict[str, Any]] | None:
    """
    Sentinel-1 L0 processing, with the settings of the `s1l0-default-setting` Prefect variable.
    The session should have been staged before.

    A `product.<product type>.generated` event is emitted for each generated product, related to the workflow of
    the processing: systematic with `env_args.process.force_systematic`, set by the automation that starts `process-l0` after the
    staging of the session.

    Args:
        session: Identifier of the session.
        env_args: Flow environment arguments. Its `process` field sets `force_systematic` (run the processing as a
            systematic one, so that the automations start the next processing of the chain from the events of the
            generated products) and `verbose` (not used yet).
    """
    return await process_staged_session("1", session, env_args)
