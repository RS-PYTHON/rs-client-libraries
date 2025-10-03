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

""" . """


from typing import Any, Dict, Iterable, Optional, List

class TaskTableError(ValueError):
    """Errors related to Task Table parsing/validation."""

def _select_unit_names(tasktable: Dict[str, Any], *, pipeline: Optional[str], unit: Optional[str]) -> List[str]:
    """
    Return the ordered list of unit names
    """
    if pipeline and unit:
        raise TaskTableError('Provide either "pipeline" or "unit", not both.')
    if not pipeline and not unit:
        raise TaskTableError('One of "pipeline" or "unit" must be provided.')

    if unit:
        # Caller asked for a single unit; existence will be validated later against units index.
        return [unit]

    # Validate pipelines shape
    if not isinstance(tasktable, dict):
        raise TaskTableError("Task table root must be a JSON object (dict).")
    if "pipelines" not in tasktable or not isinstance(tasktable["pipelines"], list):
        raise TaskTableError('Missing or invalid "pipelines" list in task table.')

    # Find pipeline by name
    pl = next((p for p in tasktable["pipelines"] if p.get("name") == pipeline), None)
    if not pl:
        raise TaskTableError(f'Pipeline "{pipeline}" not found.')

    steps = pl.get("steps")
    if not isinstance(steps, list) or not steps:
        raise TaskTableError(f'Pipeline "{pipeline}" has no steps.')

    ordered = sorted(steps, key=lambda s: s.get("order", 0))
    names = [s.get("unit_name") for s in ordered if isinstance(s, dict) and s.get("unit_name")]
    if not names:
        raise TaskTableError(f'Pipeline "{pipeline}" steps do not contain valid "unit_name" entries.')
    return names

def _build_entries(entries: List[Dict[str, Any]],
                   io_index: Dict[str, Dict[str, Any]],
                   processing_mode: Optional[Iterable[str]],
                   with_origin: bool) -> List[Dict[str, Any]]:
    """
    Build STEP 1 entries for input_products / input_adfs / output_products.
    """
    kept: List[Dict[str, Any]] = []
    mode_set = set(processing_mode) if processing_mode else None

    for e in entries or []:
        if not isinstance(e, dict):
            continue
        name = e.get("name")
        if not name:
            continue

        mode = e.get("mode")
        if mode == "always":
            pass
        elif mode is None:
            # No mode specified
            pass
        else:
            if mode_set is None or mode not in mode_set:
                continue

        out: Dict[str, Any] = {"name": name}

        # origin (only for inputs/outputs)
        if with_origin:
            origin = e.get("origin")
            if isinstance(origin, str):
                # Normalize dotted to underscored pipeline refs
                norm = {
                    "pipeline.input": "pipeline_input",
                    "pipeline.output": "pipeline_output",
                    "pipeline.internal": "pipeline_internal",
                }.get(origin, origin)
                out["origin"] = norm

        # Mandatory precedence: entry > io > unset
        if "mandatory" in e:
            out["mandatory"] = bool(e["mandatory"])

        # Merge IO config
        io_cfg = io_index.get(name, {})
        for k in ("type", "store_type", "store_params", "alternatives",
                  "opening_mode", "regex", "multiplicity"):
            if k in io_cfg:
                out[k] = io_cfg[k]

        if "mandatory" not in out and "mandatory" in io_cfg:
            out["mandatory"] = bool(io_cfg["mandatory"])

        kept.append(out)

    return kept

def build_units_list(
    tasktable: Dict[str, Any],
    pipeline: str | None = None,
    unit: str | None = None,
    processing_mode: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """
    STEP 1: Build the list of processing units from the Task Table.
    """

    if not isinstance(tasktable, dict):
        raise TaskTableError("Task table root must be a JSON object (dict).")
    if "units" not in tasktable or not isinstance(tasktable["units"], list):
        raise TaskTableError('Missing or invalid "units" list in task table.')
    if "io" not in tasktable or not isinstance(tasktable["io"], list):
        raise TaskTableError('Missing or invalid "io" list in task table.')

    # Build indices for quick lookup
    units_index: Dict[str, Dict[str, Any]] = {}
    for u in tasktable["units"]:
        if isinstance(u, dict) and isinstance(u.get("name"), str):
            units_index[u["name"]] = u
    if not units_index:
        raise TaskTableError('No valid unit entries found in "units".')

    io_index: Dict[str, Dict[str, Any]] = {}
    for io in tasktable["io"]:
        if isinstance(io, dict) and isinstance(io.get("name"), str):
            io_index[io["name"]] = io

    # Select unit names from pipeline or explicit unit
    unit_names = _select_unit_names(tasktable, pipeline=pipeline, unit=unit)

    # Build output units
    out_units: List[Dict[str, Any]] = []
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
        )
        input_adfs = _build_entries(
            udef.get("input_adfs", []),
            io_index,
            processing_mode,
            with_origin=False,
        )
        output_products = _build_entries(
            udef.get("output_products", []),
            io_index,
            processing_mode,
            with_origin=True,
        )

        out_units.append({
            "name": uname,
            "module": module,
            "input_products": input_products,
            "input_adfs": input_adfs,
            "output_products": output_products,
        })

    return {"units": out_units}
