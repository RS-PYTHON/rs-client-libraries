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

"""."""

import re
from collections.abc import Iterable
from copy import deepcopy
from typing import Any

from rs_common.utils import strftime_millis

EXTERNAL_VAR_PATTERN = re.compile(r"\{external_variable\.([a-zA-Z_][a-zA-Z0-9_]*)\}")
VARIABLE_PATTERN = re.compile(r"^{(.*)}$")  # matches exactly "{var}" (whole string)


class TaskTableError(ValueError):
    """Errors related to Task Table parsing/validation."""


def _replace_external_variables(obj, external_variables: dict[str, Any] | None):
    if isinstance(obj, dict):
        return {k: _replace_external_variables(v, external_variables) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace_external_variables(v, external_variables) for v in obj]
    if isinstance(obj, str) and external_variables:

        def replacer(match: re.Match):
            variable_name = match.group(1)
            if variable_name not in external_variables:
                raise TaskTableError(f"Unknown external variable: {variable_name}")
            value = external_variables[variable_name]
            return strftime_millis(value) if "datetime" in variable_name else str(value)

        return EXTERNAL_VAR_PATTERN.sub(replacer, obj)
    return obj


def _extract_io_origin_from_pipeline(
    unit_name: str,
    product_name: str,
    pipeline_full: dict[str, Any],
    is_input: bool = True,
    step_id: int = 0,
) -> str:
    """
    Function to extract the correct origin for the given input or output product of a pipeline step.
    This function handles fixed external origins ("pipeline.input", "pipeline.output", "pipeline.internal")
    and dynamic internal origins (basically any input that is the output of another step).
    In dynamic cases, all checks to ensure that the referenced product is correct and exists in the pipeline
    are performed.

    Args:
        unit_name: name of the unit for which we want to extract the origin of one of its products
        product_name: name of the product for which we want to extract the origin
        pipeline_full: full description of the pipeline, as defined in the tasktable
        is_input: whether the product is an input (True, default value) or an output (False)
        step_id: step_id value in case we have to discriminate between several units with the same name
    """
    # Origin name mapping
    origin_mapping = {
        "pipeline.input": "pipeline_input",
        "pipeline.output": "pipeline_output",
        "pipeline.internal": "pipeline_internal",
    }
    # Pattern for origins chaining units inside a pipeline: unit.step_id.output_product_name or unit.output_product_name
    unit_origin_pattern = r"^[^.]+\.[^.]+(\.[^.]+)?$"

    # Retrieve the details of the step defining the unit and the step_id given (if any) from the pipeline definition
    pipeline_unit: dict[str, Any] = {}
    for step in pipeline_full.get("steps", []):
        if step.get("unit_name") == unit_name:
            # If the unit name matches but NOT the given step_id, we keep searching
            if step_id and step.get("step_id") != step_id:
                continue
            pipeline_unit = step
            break

    if not pipeline_unit:
        raise TaskTableError(f"Unit '{unit_name}' not found in given pipeline: {pipeline_full}.")

    # What field to look at depending on if the product is input or output
    io_type = "input_products" if is_input else "output_products"

    # Retrieve origin for the product
    product_origin = pipeline_unit.get(io_type, {}).get(product_name)
    if not product_origin:
        raise TaskTableError(
            f"Product '{product_name}' not found in field '{io_type}' for unit '{unit_name}' "
            f"defined in pipeline. Unit is: {pipeline_unit}.",
        )

    # Case 1: origin is one of the normalized cases. We just change it to the internal name
    if product_origin in origin_mapping:
        product_origin = origin_mapping[product_origin]

    # Case 2: origin is a product from another unit
    elif re.fullmatch(unit_origin_pattern, product_origin):
        all_fields = product_origin.split(".")

        # Origin unit is always the first field.
        origin_unit_name = all_fields[0]
        # Origin step_id is the second field if there are 3, else it has to be retrieved from the units map
        # Origin product name is the last field, no matter if there are 2 or 3 fields
        if len(all_fields) == 3:
            origin_step_id = int(all_fields[1])
            origin_product_name = all_fields[2]
        else:
            origin_step_id = 0
            origin_product_name = all_fields[1]

        #  We check that origin unit name exists in the tasktable and retrieve the associated pipeline step
        origin_unit: dict[str, Any] = {}
        for step in pipeline_full.get("steps", []):
            if step.get("unit_name") == origin_unit_name:
                # If we have an origin_step_id given, check that the step found matches this value aswell.
                # If not, keep searching
                if origin_step_id and step.get("step_id") != origin_step_id:
                    continue
                origin_unit = step
                origin_step_id = step.get("step_id")
                break

        if not origin_unit:
            raise TaskTableError(
                f"Could not find unit '{origin_unit_name}' needed by input product '{product_name}' of unit "
                f"'{unit_name}' in pipeline. Available steps are: {pipeline_full.get('steps', [])}.",
            )

        # Check that the referenced product exists in the outputs of the unit we found
        if product_name not in origin_unit.get("output_products", {}):
            raise TaskTableError(
                f"Unit '{unit_name}' needs product '{product_name}' from unit '{origin_unit_name}' "
                f"but available outputs are: {origin_unit.get("output_products", {})}.",
            )

        product_origin = f"{origin_unit_name}.{origin_step_id}.{origin_product_name}"

    return product_origin


def _build_entry(
    io_product: dict,
    io_index: dict[str, dict[str, Any]],
    processing_modes: Iterable[str] | None,
    external_variables: dict[str, Any] | None,
    origin: str = "",
) -> dict[str, Any] | None:
    """
    Build a dictionary describing an input or an output, using the conditions for this I/O defined
    in the unit description and the full description of the I/O from the tasktable.
    The function returns None if the I/O does not match the current processing modes given.
    This function can be used for input_products, input_adfs or output_products.

    Args:
        io_product: content of the field "input_products" (or output, or adfs) from the unit's description
        io_index: dictionary of the I/O descriptions from the "io" section of the tasktable
        processing_modes: list of processing modes used for this flow
        origin: origin of the I/O. Needs to be computed beforehand for pipelines. Optional.
    """
    # Check that the defined product has the two mandatory fields: name and mandatory
    for field in ("name", "mandatory"):
        if field not in io_product:
            raise TaskTableError(f"Missing '{field}' in IO product definition: {io_product}")

    # Retrieve mode. If there is no "mode" field, we consider that the mode is "always" (default value)
    mode = io_product.get("mode", "always")

    # Extra fields. All fields from io_product that are not "name", "mandatory" or "mode"
    extra_fields: dict[str, Any] = {}
    for field_name, field_value in io_product.items():
        if field_name not in {"name", "mode", "mandatory"}:
            extra_fields[field_name] = field_value

    # Filter by processing mode: skip this product if it has a mode that's not "always"
    # and not in the provided processing modes
    processing_modes = processing_modes or []
    if mode != "always" and mode not in processing_modes:
        return None

    # Retrieve product details from "io" section
    io_details = io_index.get(io_product["name"], {})
    if not io_details:
        raise TaskTableError(
            f'Could not find details for product "{io_product["name"]}" in "io" section, " \
                "available products are: {io_index.keys()}.',
        )

    # Merge info from both to create the final product details
    product_details = {
        "name": io_product["name"],
        "mandatory": bool(io_product["mandatory"]),
        **extra_fields,
    }
    if origin:
        product_details["origin"] = origin
    for k, v in io_details.items():
        # When merging, we don't touch the values already set in "product_details" and also don't keep the "mode" field
        if k not in [*product_details, "mode"]:
            product_details[k] = v

    # Finally, replace the external variables with the values given
    product_details = _replace_external_variables(product_details, external_variables)

    return product_details


def _build_single_unit_details(
    unit_name: str,
    units_index: dict[str, dict[str, Any]],
    io_index: dict[str, dict[str, Any]],
    processing_modes: Iterable[str] | None,
    external_variables: dict[str, Any] | None,
    full_pipeline: dict[str, Any] | None = None,
    step_id: int = 0,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Build the details of a single unit for the payload.
    This function is suited for both "unit" and "pipeline" modes.
    In "pipeline" mode, the full pipeline definition has to be given, along with the "step_id" value
    of the step corresponding to the unit we want to build, for cases when several steps have the same unit name.

    Args:
        unit_name: name of the unit to build
        units_index: dictionary of the unit descriptions from the "units" section of the tasktable
        io_index: dictionary of the I/O descriptions from the "io" section of the tasktable
        processing_modes: list of processing modes used for this flow
        external_variables: dictionary of external variables values from flow's input
        full_pipeline: full definition of the pipeline from the tasktable.
            Optional, needed only if the flow mode is "pipeline"
        step_id: ID of the step in the pipeline corresponding to the given unit. Optional, needed only
            if the flow mode is "pipeline" and the pipeline has multiple steps using the same unit
        parameters: optional parameters included in the step definition in the pipeline, if any
    """
    # Get unit details from "units" section
    unit_details = units_index.get(unit_name)
    if not unit_details:
        raise TaskTableError(f'Unit "{unit_name}" not found in "units". Available units: {units_index.keys()}.')

    # Field "module" is simply retrieved from the unit details
    module = unit_details.get("module")
    if not isinstance(module, str) or not module:
        raise TaskTableError(f'Unit "{unit_name}" is missing a valid "module" string.')

    # Build input products, the origin of the product can be dynamic if the unit is part of a pipeline,
    # but is always the external input in "unit" mode
    input_products: list[dict[str, Any]] = []
    for input_product in unit_details.get("input_products", {}):
        if full_pipeline:
            input_product_origin = _extract_io_origin_from_pipeline(
                unit_name,
                input_product.get("name", ""),
                full_pipeline,
                is_input=True,
                step_id=step_id,
            )
        else:
            input_product_origin = "pipeline_input"

        input_entry = _build_entry(
            input_product,
            io_index,
            processing_modes,
            external_variables,
            origin=input_product_origin,
        )
        if input_entry:
            input_products.append(input_entry)

    # Build input ADFS, only defined by the unit details, does not depend on the mode
    input_adfs: list[dict[str, Any]] = []
    for adfs_product in unit_details.get("input_adfs", {}):
        adfs_entry = _build_entry(adfs_product, io_index, processing_modes, external_variables)
        if adfs_entry:
            input_adfs.append(adfs_entry)

    # Build output products, the origin of the product can be dynamic if the unit is part of a pipeline,
    # but is always the external output in "unit" mode
    output_products: list[dict[str, Any]] = []
    for output_product in unit_details.get("output_products", {}):
        if full_pipeline:
            output_product_origin = _extract_io_origin_from_pipeline(
                unit_name,
                output_product.get("name", ""),
                full_pipeline,
                is_input=False,
            )
        else:
            output_product_origin = "pipeline_output"

        output_entry = _build_entry(
            output_product,
            io_index,
            processing_modes,
            external_variables,
            origin=output_product_origin,
        )
        if output_entry:
            output_products.append(output_entry)

    # If "pipeline" mode, call the unit <unit_name>.<step_id> to be able to differenciate
    # between several steps using the same unit
    if full_pipeline:
        unit_name = f"{unit_name}.{step_id}"

    # Build the final unit details for the payload
    out_unit: dict[str, Any] = {
        "name": unit_name,
        "module": module,
        "input_products": input_products,
        "input_adfs": input_adfs,
        "output_products": output_products,
    }
    if parameters:
        out_unit["parameters"] = parameters

    return out_unit


def build_unit_list(
    tasktable: dict[str, Any],
    pipeline: str | None = None,
    unit: str | None = None,
    processing_mode: Iterable[str] | None = None,
    external_variables: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Build the list of units needed for the payload, from the tasktable given and the flow mode (pipeline or unit).
    If the mode is "unit", the list returned will only contain the requested unit, with its input and output linked
    to the external input and output.
    If the mode is "pipeline", the list will contain all the ordered units of the pipeline, with their inputs and
    outputs correctly chained if needed.

    Args:
        tasktable: dictionary containing the content of the tasktable
        pipeline: name of the pipeline to build, if the mode is "pipeline". If "unit" is given, has to be None.
        unit: name of the unit to build, if the mode is "unit". If "pipeline" is given, has to be None.
        processing_mode: list of processing modes used for this flow. Optional
        external_variables: dictionary of external variables values from flow's input. Optional
    """
    # Validate pipelines shape
    if not isinstance(tasktable, dict):
        raise TaskTableError(f"Task table root must be a JSON object (dict): {tasktable}")
    if "pipelines" not in tasktable or not isinstance(tasktable["pipelines"], list):
        raise TaskTableError(f"Missing or invalid 'pipelines' list in task table: {tasktable}")
    if "units" not in tasktable or not isinstance(tasktable["units"], list):
        raise TaskTableError(f"Missing or invalid 'units' list in task table: {tasktable}")
    if "io" not in tasktable or not isinstance(tasktable["io"], list):
        raise TaskTableError(f"Missing or invalid 'io' list in task table: {tasktable}")

    if pipeline and unit:
        raise TaskTableError("Provide either 'pipeline' or 'unit', not both.")
    if not pipeline and not unit:
        raise TaskTableError("One of 'pipeline' or 'unit' must be provided.")

    # Retrieve list of units from "units" field
    units_index: dict[str, dict[str, Any]] = {}
    for u in tasktable["units"]:
        if isinstance(u, dict) and isinstance(u.get("name"), str):
            units_index[u["name"]] = u
    if not units_index:
        raise TaskTableError('No valid unit entries found in "units".')

    # Retrieve details of inputs/outputs from "io" field
    io_index: dict[str, dict[str, Any]] = {}
    for io in tasktable["io"]:
        if isinstance(io, dict) and isinstance(io.get("name"), str):
            io_index[io["name"]] = io

    out_units: list[dict[str, Any]] = []

    if unit:
        # Handle only one unit
        unit_details = _build_single_unit_details(
            unit,
            units_index,
            io_index,
            processing_mode,
            external_variables,
        )
        out_units.append(unit_details)

    if pipeline:
        # Retrieve pipeline definition from the list of pipelines
        full_pipeline = next((p for p in tasktable.get("pipelines", []) if p.get("name") == pipeline), None)

        if not full_pipeline:
            raise TaskTableError(
                f"Could not find pipeline named '{pipeline}' in tasktable. "
                f"Existing pipelines: {tasktable.get("pipelines", [])}",
            )

        # Retrieve list of steps from the pipeline
        pipeline_steps = full_pipeline.get("steps")
        if not pipeline_steps or not isinstance(pipeline_steps, list):
            raise TaskTableError(f"Pipeline '{pipeline}' is missing a valid 'steps' list.")

        for step in pipeline_steps:
            # Validate step structure
            if not isinstance(step, dict):
                raise TaskTableError(
                    f"Step in pipeline '{pipeline}' doesn't have the expected format (expected JSON dict): {step}",
                )
            for field in ("unit_name", "step_id", "input_products", "output_products"):
                if field not in step:
                    raise TaskTableError(f"Step in pipeline '{pipeline}' is missing mandatory field '{field}': {step}")

            # Build unit details for this step
            unit_details = _build_single_unit_details(
                step["unit_name"],
                units_index,
                io_index,
                processing_mode,
                external_variables,
                full_pipeline=full_pipeline,
                step_id=step["step_id"],
                parameters=step.get("parameters", None),
            )

            out_units.append(unit_details)

    return out_units


def build_cql2_json(query: dict[str, Any], values: dict[str, Any]):
    """
    Recursively replaces placeholders of the form {var} in a dictionary or list
    using the mapping from 'values'.
    """

    def _replace(item):
        if isinstance(item, str):
            # replace if found, else keep
            match = VARIABLE_PATTERN.match(item)
            return values.get(match.group(1), item) if match else item
        if isinstance(item, list):
            return [_replace(x) for x in item]
        if isinstance(item, dict):
            return {k: _replace(v) for k, v in item.items()}
        return item

    # Work on a deep copy so we don't mutate the original
    return _replace(deepcopy(query))["stac"]
