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

"""."""


import re
from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime
from typing import Any

from rs_common.utils import strftime_millis


class TaskTableError(ValueError):
    """Errors related to Task Table parsing/validation."""


def _replace_external_variables(obj, start_datetime, end_datetime):
    if isinstance(obj, dict):
        return {k: _replace_external_variables(v, start_datetime, end_datetime) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace_external_variables(v, start_datetime, end_datetime) for v in obj]
    if isinstance(obj, str):
        if obj == "{external_variable.start_datetime}":
            return strftime_millis(start_datetime)
        if obj == "{external_variable.end_datetime}":
            return strftime_millis(end_datetime)
        return obj
    return obj


def _select_unit_names(tasktable: dict[str, Any], *, pipeline: str | None) -> list[str]:
    """
    Return the ordered list of unit names
    """
    # Find pipeline by name
    pl = next((p for p in tasktable["pipelines"] if p.get("name") == pipeline), None)
    if not pl:
        available = [p.get("name") for p in tasktable.get("pipelines", []) if p.get("name")]
        available_str = ", ".join([f'"{n}"' for n in available]) if available else '"<none>"'
        raise TaskTableError(f'Pipeline "{pipeline}" not found. Available pipelines: {available_str}')

    steps = pl.get("steps")
    if not isinstance(steps, list) or not steps:
        raise TaskTableError(f'Pipeline "{pipeline}" has no steps.')

    ordered = sorted(steps, key=lambda s: s.get("order", 0))
    names: list[str] = [s["unit_name"] for s in ordered if isinstance(s, dict) and isinstance(s.get("unit_name"), str)]
    if not names:
        raise TaskTableError(f'Pipeline "{pipeline}" steps do not contain valid "unit_name" entries.')
    return names


def _build_entries(
    entries: list[dict[str, Any]],
    io_index: dict[str, dict[str, Any]],
    processing_mode: Iterable[str] | None,
    with_origin: bool,
    *,
    unit_name: str,
    origin_kind: str,
    origin_map_by_unit: dict[str, dict[str, dict[str, Any]]],
    start_datetime: datetime | None,
    end_datetime: datetime | None,
) -> list[dict[str, Any]]:
    """
    Build STEP 1 entries for input_products / input_adfs / output_products.
    """
    kept: list[dict[str, Any]] = []
    mode_set = set(processing_mode) if processing_mode else None

    for e in entries or []:
        if not isinstance(e, dict):
            continue
        name = e.get("name")
        if not name:
            continue

        # Mode filter: keep items with mode="always" or without mode;
        # if processing_mode is None, drop items that specify a mode;
        # otherwise keep only if mode ∈ processing_mode.
        mode = e.get("mode")
        if mode == "always":
            pass
        elif mode is None:
            # No mode specified
            pass
        else:
            if mode_set is None or mode not in mode_set:
                continue

        out: dict[str, Any] = {"name": name}

        # origin (only for inputs/outputs)
        if with_origin:
            origin_raw = origin_map_by_unit.get(unit_name, {}).get(origin_kind, {}).get(name)
            if isinstance(origin_raw, str):
                # Normalize dotted to underscored pipeline refs
                norm = {
                    "pipeline.input": "pipeline_input",
                    "pipeline.output": "pipeline_output",
                    "pipeline.internal": "pipeline_internal",
                }.get(origin_raw, origin_raw)
                out["origin"] = norm

        if "mandatory" not in e:
            raise TaskTableError(f'Missing "mandatory" for item "{name}" in unit "{unit_name}".')
        out["mandatory"] = bool(e["mandatory"])

        # Merge IO config
        io_cfg = io_index.get(name, {}) or {}
        merged_cfg: dict[str, Any] = {}
        for k, v in io_cfg.items():
            if k in ("name", "mandatory"):
                continue
            merged_cfg[k] = v

        if merged_cfg:
            merged_cfg = _replace_external_variables(merged_cfg, start_datetime, end_datetime)
            out.update(merged_cfg)

        kept.append(out)

    return kept


def build_unit_list(
    tasktable: dict[str, Any],
    pipeline: str | None = None,
    unit: str | None = None,
    processing_mode: Iterable[str] | None = None,
    *,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
) -> dict[str, Any]:
    """
    STEP 1: Build the list of processing units from the Task Table.
    """
    # Validate pipelines shape
    if not isinstance(tasktable, dict):
        raise TaskTableError("Task table root must be a JSON object (dict).")
    if "pipelines" not in tasktable or not isinstance(tasktable["pipelines"], list):
        raise TaskTableError('Missing or invalid "pipelines" list in task table.')
    if "units" not in tasktable or not isinstance(tasktable["units"], list):
        raise TaskTableError('Missing or invalid "units" list in task table.')
    if "io" not in tasktable or not isinstance(tasktable["io"], list):
        raise TaskTableError('Missing or invalid "io" list in task table.')

    # Build indices for quick lookup
    units_index: dict[str, dict[str, Any]] = {}
    for u in tasktable["units"]:
        if isinstance(u, dict) and isinstance(u.get("name"), str):
            units_index[u["name"]] = u
    if not units_index:
        raise TaskTableError('No valid unit entries found in "units".')

    io_index: dict[str, dict[str, Any]] = {}
    for io in tasktable["io"]:
        if isinstance(io, dict) and isinstance(io.get("name"), str):
            io_index[io["name"]] = io

    if pipeline and unit:
        raise TaskTableError('Provide either "pipeline" or "unit", not both.')
    if not pipeline and not unit:
        raise TaskTableError('One of "pipeline" or "unit" must be provided.')

    # Select unit names from pipeline or explicit unit
    if unit:
        unit_names = [unit]
        if unit not in units_index:
            units = list(units_index)
            available = ", ".join(f"'{u}'" for u in units)
            raise TaskTableError(f'Unit "{unit}" not found in "units". Available units: {available}.')
    else:
        unit_names = _select_unit_names(tasktable, pipeline=pipeline)

    # pipeline provided: per-step origin maps for each unit in the selected pipeline.
    origin_map_by_unit: dict[str, dict[str, dict[str, Any]]] = {}
    if pipeline:
        pl = next((p for p in tasktable.get("pipelines", []) if p.get("name") == pipeline), None)
        steps = pl.get("steps", []) if pl else []
        for s in steps:
            if not isinstance(s, dict):
                continue
            uname = s.get("unit_name")
            if not uname:
                continue
            origin_map_by_unit[uname] = {
                "in": s.get("input_products", {}) or {},
                "out": s.get("output_products", {}) or {},
            }
    else:
        # unit provided: take the first pipeline step that defines this unit
        s = None
        for p in tasktable.get("pipelines", []):
            if not isinstance(p, dict):
                continue
            for st in p.get("steps") or []:
                if isinstance(st, dict) and st.get("unit_name") in unit_names:
                    s = st
                    break
            if s:
                break

        if not s:
            raise TaskTableError(f'No pipeline step found for unit "{unit_names[0]}" to derive origins.')

        uname = s.get("unit_name")
        origin_map_by_unit[uname] = {
            "in": s.get("input_products", {}) or {},
            "out": s.get("output_products", {}) or {},
        }

    # Build output units
    out_units: list[dict[str, Any]] = []
    for uname in unit_names:
        udef = units_index.get(uname)
        if not udef:
            raise TaskTableError(f'Unit "{uname}" not found in "units".')

        module = udef.get("module")
        if not isinstance(module, str) or not module:
            raise TaskTableError(f'Unit "{uname}" is missing a valid "module" string.')

        input_products = _build_entries(
            udef.get("input_products", []),
            io_index,
            processing_mode,
            with_origin=True,
            unit_name=uname,
            origin_kind="in",
            origin_map_by_unit=origin_map_by_unit,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        input_adfs = _build_entries(
            udef.get("input_adfs", []),
            io_index,
            processing_mode,
            with_origin=False,
            unit_name=uname,
            origin_kind="in",
            origin_map_by_unit=origin_map_by_unit,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        output_products = _build_entries(
            udef.get("output_products", []),
            io_index,
            processing_mode,
            with_origin=True,
            unit_name=uname,
            origin_kind="out",
            origin_map_by_unit=origin_map_by_unit,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

        out_units.append(
            {
                "name": uname,
                "module": module,
                "input_products": input_products,
                "input_adfs": input_adfs,
                "output_products": output_products,
            },
        )

    return {"units": out_units}


def build_cql2_json(task_table, query_name, values):
    """
    Recursively replaces placeholders of the form {var} in a dictionary or list
    using the mapping from 'values'.
    """
    template = {}
    for cql_filter in task_table["queries"]:
        if cql_filter["name"] == query_name:
            # Work on a deep copy so we don't mutate the original
            template = deepcopy(cql_filter)
    pattern = re.compile(r"^{(.*)}$")  # matches exactly "{var}" (whole string)

    def _replace(item):
        if isinstance(item, str):
            match = pattern.match(item)
            if match:
                key = match.group(1)
                return values.get(key, item)  # replace if found, else keep
            return item
        if isinstance(item, list):
            return [_replace(x) for x in item]
        if isinstance(item, dict):
            return {k: _replace(v) for k, v in item.items()}
        return item

    return _replace(template)
