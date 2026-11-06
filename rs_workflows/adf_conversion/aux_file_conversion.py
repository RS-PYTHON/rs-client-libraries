# Copyright 2026 B-Open
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Literal, TypeAlias, Union
from xml.etree import ElementTree

import dpath
import numpy as np
import numpy.typing as npt
import xarray as xr
from pydantic import alias_generators

LOGGER = logging.getLogger(__file__)

# alias type for xml parsing functions
ParsedValue: TypeAlias = Union[bool, str, int, float, list[Union[int, float]], dict]


######################################################
#                                                    #
#  GENERAL XML TO JSON CONVERSION RELATED FUNCTIONS  #
#                                                    #
######################################################


def is_float(s: str) -> bool:
    """Check if a string can be converted into a float."""
    try:
        float(s)
        return True
    except ValueError:
        return False


def custom_snake_case(text: str) -> str:
    """Convert strings with spaces within adfs into a more suitable one.

    Compare this conversion with the standard snake_case:
        "BRC 0" ---> "brc_0" instead of "brc 0"
        "BAQ 3-Bit" ---> "baq_3bit" instead of "baq 3-bit"
    """
    text_to_snake = alias_generators.to_snake(text)
    return text_to_snake.replace(" ", "_").replace("-", "")


def build_leaf(leaf: ElementTree.Element) -> ParsedValue:
    """Convert the content from an element tree leaf in the xml structure into an appropriate python data type."""
    text = leaf.text.strip() if leaf.text else ""
    if text.lower() == "true":
        return True
    elif text.lower() == "false":
        return False
    elif is_float(text):
        if "." in text or "e" in text.lower():
            return float(text)
        return int(text)
    elif all(t.isnumeric() for t in text.split()):
        return [int(t) for t in text.split()]
    elif all(is_float(t) for t in text.split()):
        return [float(t) for t in text.split()]
    return text


def parse_element(element: ElementTree.Element) -> dict:
    """Recursively converts an XML element and its children into a Python dictionary."""
    # check if within the children there are repeated tags, meaning we have to build a list
    children = list(element)
    tag_count: dict = {}
    for child in children:
        tag_count[child.tag] = tag_count.get(child.tag, 0) + 1
    # parse each child and build the result
    result: dict = {}
    for child in children:
        # check if child is a leaf
        if not list(child):
            parsed_child = build_leaf(child)
        else:
            parsed_child = parse_element(child)
        if element.tag.lower().endswith("list") or tag_count[child.tag] > 1:
            result.setdefault(child.tag, []).append(parsed_child)
        else:
            result[child.tag] = parsed_child
    return result


def adjust_list_keys(data_dict: dict) -> dict:
    """Recursively simplify nested dictionaries by removing unnecessary intermediate keys when a list is the sole value.

    Example
    -------
    Input:
        data_dict = {
            "key_name_list": {
                "key_name": [
                    {"a": 1},
                    {"b": 2}
                ]
            }
        }

    Output:
        {
            "key_name_list": [
                {"a": 1},
                {"b": 2}
            ]
        }

    Parameters
    ----------
    data_dict : dict
        The input dictionary potentially containing unnecessarily nested list structures.

    Returns
    -------
    dict
        A dictionary with simplified structure, where list-wrapping keys have been collapsed.
    """
    for key, val in data_dict.items():
        # the list are stored inside dictionary with 1 key and then the list of dictionaries
        if isinstance(val, dict) and len(val) == 1:
            k = next(iter(val.keys()))
            if isinstance(val[k], list) and all(isinstance(elem, dict) for elem in val[k]):
                data_dict[key] = [adjust_list_keys(elem) for elem in val[k]]
            elif isinstance(val[k], dict):
                data_dict[key] = adjust_list_keys(val[k])
        elif isinstance(val, dict):
            data_dict[key] = adjust_list_keys(val)
        else:
            continue
    return data_dict


def adjust_camel_to_snake_case(data: Any) -> Any:
    """Convert recursively the keys into the give dictionary from camelCase to snake_case."""
    if isinstance(data, dict):
        ret = {custom_snake_case(k): adjust_camel_to_snake_case(v) for k, v in data.items()}
    elif isinstance(data, list):
        ret = [adjust_camel_to_snake_case(item) for item in data]
    else:
        ret = data
    return ret


def _parse_xml_tree(input_path: str) -> Any:
    """Parse XML from a local path or an ``s3://`` URI (via fsspec)."""
    if input_path.startswith("s3://"):
        import fsspec

        with fsspec.open(input_path, "rb") as handle:
            return ElementTree.parse(handle)
    return ElementTree.parse(input_path)


def convert_adf_xml_to_dict(input_path: str) -> dict:
    """End-to-end function to convert a generic xml file into a dictionary.

    Parameters
    ----------
    input_path : str
        Path to the input XML file (local path or ``s3://`` URI).

    Returns
    -------
    dict
        A cleaned, snake_case-formatted dictionary representation of the XML structure.
    """
    tree = _parse_xml_tree(input_path)
    root = tree.getroot()
    if root is None:
        raise ValueError(f"No root element in XML file: {input_path}")
    data = parse_element(root)
    data = adjust_list_keys(data)
    data = adjust_camel_to_snake_case(data)
    return data


def save_json_output(data_dict: dict, json_output_path: str) -> None:
    """Save the dictionary obtained from the XML conversion into a JSON file.

    Parameters
    ----------
    data_dict : dict
        Dictionary to be serialized and saved.

    json_output_path : str
        Local path or ``s3://`` URI for the output JSON file. Parent directories
        are created automatically for local paths.
    """
    if json_output_path.startswith("s3://"):
        import fsspec

        with fsspec.open(json_output_path, "w") as handle:
            json.dump(data_dict, handle, indent=4)
    else:
        out = Path(json_output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w") as handle:
            json.dump(data_dict, handle, indent=4)
    LOGGER.info(f"File saved in {json_output_path}")


################################################
#                                              #
#  AUX PP1, PP2, INS and CAL JSON ADJUSTMENTS  #
#                                              #
################################################

bit_type_short_name = {
    "16 bit Signed Integer": "int16",
    "16 bit Unsigned Integer": "uint16",
    "8 bit Signed Integer": "int8",
    "8 bit Unsigned Integer": "uint8",
}

# Maps original list keys to a tuple of:
# - new key name to be used in output
# - a function that extracts the new key from each element
LIST_REMOVAL_FUNCTIONS: dict[str, tuple[str, Callable[[dict], str]]] = {
    # INS related fields
    "swath_params_list": ("swath_params", lambda x: x.pop("swath")),
    "on_board_decimation_filter_params_list": (
        "on_board_decimation_filter_params_by_rx_pol",
        lambda x: x.pop("rx_polarisation"),
    ),
    "rx_variation_correction_params_list": (
        "rx_variation_correction_params_by_rx_pol",
        lambda x: x.pop("rx_polarisation"),
    ),
    "rcc_params_list": ("rcc_params", lambda x: custom_snake_case(x.pop("signal"))),
    "internal_calibration_params_list": (
        "internal_calibration_params",
        lambda x: x.pop("swath"),
    ),
    "replica_pcc_params_list": (
        "image_signals",
        lambda x: custom_snake_case(x.pop("signal")),
    ),
    "pg_pcc_params_list": ("pg_signals", lambda x: custom_snake_case(x.pop("signal"))),
    "timeline_list": ("timeline_by_ecc", lambda x: x.pop("ecc_number")),
    "sequence_list": ("", lambda x: custom_snake_case(x.pop("name"))),
    "swath_map_list": ("swath_number_to_swath", lambda x: x.pop("swath_number")),
    "huffman_lut_list": ("huffman_lut", lambda x: custom_snake_case(x.pop("baq_code"))),
    "nrl_lut_list": ("nrl_lut", lambda x: custom_snake_case(x.pop("baq_code"))),
    "srl_lut_list": ("srl_lut", lambda x: custom_snake_case(x.pop("baq_code"))),
    "threshold_lut_list": (
        "threshold_lut",
        lambda x: custom_snake_case(x.pop("baq_code")),
    ),
    # PP1 and PP2 related fields
    "product_list": ("product", lambda x: x.pop("product_id")),
    "azi_proc_block_params_list": ("azi_proc_block_params", lambda x: x.pop("swath")),
    "range_params_list": ("range_params", lambda x: x.pop("swath")),
    "azimuth_params_list": ("azimuth_params", lambda x: x.pop("swath")),
    "application_lut_list": (
        "application_lut",
        lambda x: custom_snake_case(x.pop("application_lut_id")),
    ),
    "scaling_lut_list": (
        "scaling_lut",
        lambda x: bit_type_short_name[x.pop("output_pixels")],
    ),
    # CAL related fields
    "calibration_params_list": (
        "internal_calibration_params",
        lambda x: x.pop("swath"),
    ),
}


def convert_lists_to_dicts(original_key: str, list_of_dict: list[dict]) -> tuple[str, dict]:
    """Convert a list of dictionaries into a dictionary, based on a rule defined by the key.

    For some keys (e.g., 'calibration_params_list'), this function maps each item in the list to a new key via a
    function in LIST_REMOVAL_FUNCTIONS, optionally handling nested structures.

    Special handling is done for:
        - keys ending with "calibration_params_list": grouped by new key and polarisation
        - key == "isp_list": values grouped under '<key>_sequence'

    Parameters
    ----------
    original_key : str
        The key determining the conversion strategy.
    list_of_dict : list of dict
        List of dictionaries to be converted.

    Returns
    -------
    tuple of (str, dict)
        A tuple containing:
        - final_key : str
            A key to use in the final structure (may be empty).
        - list_converted_to_dict : dict
            The transformed dictionary.
    """

    # `values` is a list of dictionaries with same keys
    list_converted_to_dict: dict = {}
    if original_key in LIST_REMOVAL_FUNCTIONS:
        final_key, map_func = LIST_REMOVAL_FUNCTIONS[original_key]
        new_keys_list = [map_func(t) for t in list_of_dict]
        # build the new value as dict instead of a list
        for new_key, val in zip(new_keys_list, list_of_dict):
            # in the internal calibration params case we need sub-dicts base on swath
            if original_key.endswith("calibration_params_list"):
                pol_dict = {val.pop("polarisation"): val}
                list_converted_to_dict.setdefault(new_key, {}).update(pol_dict)
            else:
                if isinstance(val, dict) and len(val) == 1 and "values" in val:
                    # remove the dict like {"values": [...]}
                    val = val["values"]
                list_converted_to_dict[new_key] = val
    elif original_key == "isp_list":
        final_key = ""
        for dict_in_list in list_of_dict:
            for key, val in dict_in_list.items():
                key_sequence = key + "_sequence"
                list_converted_to_dict.setdefault(key_sequence, []).append(val)
    else:
        final_key = ""
        LOGGER.warning(f"The element with {original_key=} is not converted into a dictionary.")
    return final_key, list_converted_to_dict


# TODO: use more clear names for this functions
def adjust_converted_dict(final_key: str | int, list_converted_to_dict: dict) -> dict:
    """Adjust a converted dictionary by wrapping it under a final key if needed.

    If `final_key` is provided, the dictionary is wrapped under that key. Special handling is applied when `final_key`
    is "swath_number_to_swath": the nested dictionaries are flattened by extracting the "swath" value.

    Parameters
    ----------
    final_key : str or int
        The key under which to wrap the converted dictionary. If falsy (e.g., None or empty string), no wrapping is
        performed.
    list_converted_to_dict : dict
        The dictionary resulting from a previous list-to-dict conversion.

    Returns
    -------
    dict
        The possibly adjusted dictionary, either wrapped under `final_key` or unchanged.
    """
    if not final_key:
        return list_converted_to_dict
    elif final_key == "swath_number_to_swath":
        for key in list_converted_to_dict.keys():
            list_converted_to_dict[key] = list_converted_to_dict[key]["swath"]
        return {final_key: list_converted_to_dict}
    return {final_key: list_converted_to_dict}


# TODO: use the keys HH HV VV VH for the gain list in aux_pp1 -> slc_proc_params -> swath_params
# TODO: merge "scaling_lut" with previous key inside aux_pp1 -> application_lut : unique keys are useless.
def adjust_dict_structure(data_dict: dict) -> dict:
    """Recursively convert elements ending with '_list' in a nested dictionary.

    This function traverses a nested dictionary converting all keys ending with '_list' into dictionaries using
    `convert_lists_to_dicts` and `adjust_converted_dict`. The process is applied recursively to nested dictionaries.

    Parameters
    ----------
    data_dict : dict
        The nested dictionary to process and convert.

    Returns
    -------
    dict
        A new dictionary with all applicable '_list' entries converted
        and nested
    """
    final_dict = {}
    for key, values in data_dict.items():
        if isinstance(key, str) and key.endswith("_list"):
            final_key, list_converted_to_dict = convert_lists_to_dicts(key, values)
            converted_dict = adjust_converted_dict(final_key, list_converted_to_dict)
            # recurse over inner layers
            final_dict.update(adjust_dict_structure(converted_dict))
        elif isinstance(data_dict[key], dict):
            # recurse over nested dicts
            final_dict[key] = adjust_dict_structure(data_dict[key])
        else:
            # in all other cases, do not apply any change
            final_dict[key] = data_dict[key]
    return final_dict


AuxJsonAdfType = Literal["pp1", "pp2", "ins"]


def adjust_aux_pp1_common_proc_params(data_dict: dict) -> dict:
    """Restructure ``common_proc_params`` in a normalised AUX PP1 dict.

    Legacy XML stores ellipsoid parameters and ``use_dem_flag`` inside
    ``ellipsoid_params``. The reprocessed format keeps only ``use_dem_flag`` at
    the ``common_proc_params`` level; ellipsoid fields are provided by
    ``S00__ADF_CONST`` instead.
    """
    for product_params in data_dict["product"].values():
        common = product_params["common_proc_params"]
        ellipsoid = common.pop("ellipsoid_params")
        common["use_dem_flag"] = ellipsoid["use_dem_flag"]
    return data_dict


def save_aux_xml_to_json(
    input_path: str,
    json_output_path: str,
    *,
    adf_type: AuxJsonAdfType | None = None,
) -> None:
    """Convert an AUX PP1/PP2/INS XML file to JSON.

    Parameters
    ----------
    input_path : str
        Path to the input XML file (local path or ``s3://`` URI).
    json_output_path : str
        Destination JSON path (local path or ``s3://`` URI).
    adf_type : {"pp1", "pp2", "ins"}
        ADF family being converted. PP1-specific post-processing is applied only
        when ``adf_type`` is ``"pp1"``.
    """
    data_dict = convert_adf_xml_to_dict(input_path)
    data_dict = adjust_dict_structure(data_dict)
    if adf_type == "pp1":
        data_dict = adjust_aux_pp1_common_proc_params(data_dict)
    save_json_output(data_dict, json_output_path)


#########################################
#                                       #
# AUX CAL CONVERSION RELATED FUNCTIONS  #
#                                       #
#########################################


def compute_antenna_angle(count: int, increment: float) -> npt.NDArray[np.float64]:
    """Compute the angles of reference for Antenna Pattern values.

    The angle range is symmetric around 0. The first value corresponds to -((count - 1)/2) * increment degrees, and the
    last to +((count - 1)/2) * increment.

    input:
        count (int): the number of data entries
        increment (float): the step size of the range

    output:
        A list containing antenna angles
    """
    if count == 1 and increment == 0:
        return np.array([0])
    return np.arange(
        ((-1) * (count - 1) / 2) * increment,
        ((count - 1) / 2) * increment + increment,
        increment,
        dtype=np.float64,
    )


def aggregate_to_complex(data: list[float]) -> list[complex]:
    """Aggregates floating point values into complex numbers. The expected order is I Q I Q I Q...

    Parameters
    ----------
        data (List): the set of floating point data

    Returns
    -------
        A list of complex values
    """
    # Make sure there is an even number of values
    if len(data) % 2 != 0:
        raise ValueError("Expected a list of even length.")
    elif (len(data) // 2) % 2 != 1:
        LOGGER.warning(
            "The elevation antenna pattern values are even.\n"
            "This means that the sequence does not have a center, which should correspond to the referenceAntennaAngle"
            "in the Roll Steering parameters described in the Instrument auxiliary file.",
        )
    complex_data = [re + im * 1j for re, im in zip(data[::2], data[1::2])]
    return complex_data


def get_antenna_pattern_da(
    data: list[complex],
    coords: npt.NDArray[np.float64],
    name: str | None = None,
    coord_name: str = "angle",
    attrs: dict | None = None,
):
    """Create an xarray DataArray from antenna pattern values.

    Parameters
    ----------
    data : array-like
        Antenna pattern values.
    coords : array-like
        Coordinate values corresponding to the antenna pattern.
    name : str, optional
        Name of the DataArray. Default is None.
    coord_name : str, optional
        Name of the coordinate dimension. Default is "angle".
    attrs : dict, optional
        Attributes to attach to the DataArray. Default is None.

    Returns
    -------
    xr.DataArray
        The constructed xarray DataArray object.
    """
    antenna_pattern_xr = xr.DataArray(
        data=data,
        dims=[coord_name],
        coords={coord_name: coords},
        name=name,
    )
    if isinstance(attrs, dict):
        antenna_pattern_xr.attrs.update(attrs)
    return antenna_pattern_xr


def convert_aux_cal_dict_to_datatree(norm_dict: dict) -> xr.DataTree:
    """Convert an AUX CAL normalized dict. The output is an xr.DataTree.

    Parameters
    ----------
    norm_dict: dict
        the structured normalized dict of a parsed xml file.

    Returns
    -------
    xr.DataTree
        Data of the dict rearranged and processed into a DataTree.
    """
    # dictionary in which to store the data
    data_dict = {}

    # loop over the different elements
    for swath, products_by_pol in norm_dict["internal_calibration_params"].items():
        # get the polarisation
        for polarisation, product in products_by_pol.items():
            # get the  actual data
            elev_antenna = product["elevation_antenna_pattern"]
            azimuth_antenna = product["azimuth_antenna_pattern"]
            azimuth_antenna_elm = product["azimuth_antenna_element_pattern"]
            abs_cal_const = product["absolute_calibration_constant"]
            noise_cal_fact = product["noise_calibration_factor"]
            # elevation antenna pattern data (even length = I/Q pairs)
            elev_values = elev_antenna["values"]
            if isinstance(elev_values, list) and len(elev_values) % 2:
                raise ValueError("legacy AUX_CAL: odd elevation pattern length")
            elev_antenna_data = aggregate_to_complex(elev_values)
            elev_antenna_increment = elev_antenna["elevation_angle_increment"]
            elev_antenna_da = get_antenna_pattern_da(
                elev_antenna_data,
                compute_antenna_angle(len(elev_antenna_data), elev_antenna_increment),
                coord_name="angle",
                name="elevation_antenna_pattern",
                attrs={
                    "beam_nominal_near_range": elev_antenna["beam_nominal_near_range"],
                    "beam_nominal_far_range": elev_antenna["beam_nominal_far_range"],
                },
            )
            # azimuth antenna pattern data
            azimuth_antenna_data = azimuth_antenna["values"]
            azimuth_antenna_increment = azimuth_antenna["azimuth_angle_increment"]
            azimuth_antenna_da = get_antenna_pattern_da(
                azimuth_antenna_data,
                compute_antenna_angle(len(azimuth_antenna_data), azimuth_antenna_increment),
                coord_name="angle",
                name="azimuth_antenna_pattern",
                attrs={"units": "dB"},
            )
            # azimuth antenna element pattern
            azimuth_antenna_elm_increment = azimuth_antenna_elm["azimuth_angle_increment"]
            azimuth_antenna_elm_data = azimuth_antenna_elm["values"]
            if not isinstance(azimuth_antenna_elm_data, list):
                azimuth_antenna_elm_data = [azimuth_antenna_elm_data]
            azimuth_antenna_elm_da = get_antenna_pattern_da(
                azimuth_antenna_elm_data,
                compute_antenna_angle(len(azimuth_antenna_elm_data), azimuth_antenna_elm_increment),
                coord_name="angle",
                name="azimuth_antenna_element_pattern",
                attrs={"units": "dB"},
            )

            # Define the current group
            group = "/".join([swath, polarisation]).lstrip("/")
            for da in [elev_antenna_da, azimuth_antenna_da, azimuth_antenna_elm_da]:
                data_dict["/".join([group, da.name])] = da.to_dataset()

            # Add the attributes
            data_dict[group] = xr.Dataset(
                attrs={
                    "absolute_calibration_constant": abs_cal_const,
                    "noise_calibration_factor": noise_cal_fact,
                },
            )

    # create the final datatree
    aux_cal_dt = xr.DataTree.from_dict(data_dict)
    return aux_cal_dt


def save_aux_cal_xml_to_zarr(input_xml: str, zarr_output_path: str) -> None:
    """Parse an AUX_CAL XML file and save its contents to a Zarr store.

    This function performs the full pipeline:
    - Parses the AUX_CAL XML file into a Python dictionary.
    - Normalizes and restructures the dictionary to a standard format.
    - Converts the normalized dictionary into an xarray DataTree.
    - Writes the DataTree to disk in Zarr format.

    Parameters
    ----------
    input_xml : str
        Path to the AUX_CAL XML input file.
    zarr_output_path : str
        Destination directory path where the Zarr store will be saved.
    """
    # NOTE: maybe we should add a check that input_xml is either a ins, pp1 or pp2 aux file.
    # create the datatree
    parsed_dict = convert_adf_xml_to_dict(input_xml)
    norm_dict = adjust_dict_structure(parsed_dict)
    aux_cal_dt = convert_aux_cal_dict_to_datatree(norm_dict)
    # save the datatree to zarr
    aux_cal_dt.to_zarr(
        zarr_output_path,
        mode="a",
        consolidated=False,
    )
    LOGGER.info(f"File saved in {zarr_output_path}")


##########################################
#                                        #
#  AUX ORB CONVERSION RELATED FUNCTIONS  #
#                                        #
##########################################

# attributes for the orb dataset
ATTRS_POSITION = {
    "long_name": "ECEF position",
    "comment": "Position vector record. This record contains the platform position data with respect to the Earth-fixed\
        reference frame",
    "units": "m",
}

ATTRS_VELOCITY = {
    "long_name": "ECEF velocity",
    "comment": "Velocity vector record. This record contains the platform velocity data with respect to the Earth-fixed\
        reference frame",
    "units": "m s-1",
}

ATTRS_ABS_ORBIT = {
    "long_name": "Absolute orbit",
    "comment": "Absolute orbit counter",
}

ATTRS_QUALITY = {
    "long_name": "Quality",
    "comment": "Quality flag indicating the quality status of the product",
}


def _orb_quality_array(values: list) -> np.ndarray:
    """Build a Zarr-safe quality array from legacy OSV quality values.

    Numeric flags are stored as ``int32``; textual flags such as ``NOMINAL`` are
    stored as an object-dtype array to avoid unstable fixed-length UTF dtypes in
    Zarr v3.
    """
    if not values:
        return np.asarray(values, dtype=np.int32)
    if all(isinstance(value, (int, np.integer)) for value in values):
        return np.asarray(values, dtype=np.int32)
    if all(isinstance(value, str) and value.isdigit() for value in values):
        return np.asarray([int(value) for value in values], dtype=np.int32)
    return np.asarray(values, dtype=object)


def string_to_datetime(string: str, prefix: str = "=") -> np.ndarray:
    """Converts a string containing a datetime to a NumPy datetime64[us] array.

    The input string is expected to have a datetime value following a specified prefix.
    The datetime part of the string is then converted to a NumPy datetime64 array with microsecond precision.

    Parameters
    ----------
    string: str
        The input string containing the datetime.
    prefix: str, optional
        The prefix preceding the datetime in the string. Default is "=".

    Returns
    -------
    np.ndarray:
        A NumPy datetime64 array representing the parsed datetime with microsecond precision.
    """
    timestamp = string.split(prefix, 1)[1].replace("T", " ")
    return np.datetime64(timestamp).astype("datetime64[ns]")


def get_orb_attrs(data_dict: dict) -> dict:
    """Extract selected attributes from the 'earth_explorer_header' section of the input dictionary.

    The function retrieves various metadata fields from nested paths inside `data_dict` and cleans some string values by
    removing the "UTC=" prefix.

    Parameters
    ----------
    data_dict : dict
        Dictionary containing the Earth Explorer header information.

    Returns
    -------
    dict
        A dictionary containing extracted attributes such as file name, mission, validity period, creator info, and
        reference frame.
    """
    earth_explorer = data_dict["earth_explorer_header"]
    attrs = {
        "file_name": dpath.get(earth_explorer, "fixed_header/file_name"),
        "description": dpath.get(earth_explorer, "fixed_header/file_description"),
        "mission": dpath.get(earth_explorer, "fixed_header/mission"),
        "file_class": dpath.get(earth_explorer, "fixed_header/file_class"),
        "file_type": dpath.get(earth_explorer, "fixed_header/file_type"),
        "validity_start": dpath.get(earth_explorer, "fixed_header/validity_period/validity_start"),
        "validity_stop": dpath.get(earth_explorer, "fixed_header/validity_period/validity_stop"),
        "file_version": dpath.get(earth_explorer, "fixed_header/file_version"),
        "source_system": dpath.get(earth_explorer, "fixed_header/source/system"),
        "source_creator": dpath.get(earth_explorer, "fixed_header/source/creator"),
        "creator_version": dpath.get(earth_explorer, "fixed_header/source/creator_version"),
        "creation_date": dpath.get(earth_explorer, "fixed_header/source/creation_date"),
        "ref_frame": dpath.get(earth_explorer, "variable_header/ref_frame"),
        "time_reference": dpath.get(earth_explorer, "variable_header/time_reference"),
    }
    for key in ["validity_start", "validity_stop", "creation_date"]:
        value = attrs[key]
        if isinstance(value, str):
            attrs[key] = value.replace("UTC=", "")  # or maybe string_to_datetime
    return attrs


def get_aux_orb_data_block(data_dict: dict) -> dict[str, list]:
    """Extract and convert the 'osv' list from the data block into a dictionary of lists.

    For keys related to time ('tai', 'utc', 'ut1'), the function converts string timestamps to datetime objects using
    `string_to_datetime`. Other keys are collected as-is into lists.

    Parameters
    ----------
    data_dict : dict
        Nested dictionary expected to contain the 'data_block' with 'osv' list of dictionaries.

    Returns
    -------
    dict
        Dictionary where each key corresponds to a list of values extracted from the original 'osv' list. Time keys are
        converted to datetime objects.
    """
    osv_original_list = data_dict["data_block"]["osv"]
    osv_keys = list(osv_original_list[0].keys())
    osv_converted_to_dict: dict = {}
    time_keys = ["tai", "utc", "ut1"]
    for osv_key in osv_keys:
        if osv_key in time_keys:
            osv_converted_to_dict[osv_key] = [
                string_to_datetime(osv_dict_item[osv_key]) for osv_dict_item in osv_original_list
            ]
        else:
            osv_converted_to_dict[osv_key] = [osv_dict_item[osv_key] for osv_dict_item in osv_original_list]
    return osv_converted_to_dict


def convert_aux_orb_dict_to_dataset(data_dict: dict) -> xr.Dataset:
    """Convert auxiliary orbit XML data to a Zarr dataset and store it.

    This function parses an input XML file containing orbit data, extracts relevant attributes and orbit vectors,
    constructs an xarray Dataset with position, velocity, absolute orbit, and quality data arrays, and saves the dataset
    to a specified Zarr store path.

    Parameters
    ----------
    input_xml : str
        Path to the input XML file containing auxiliary orbit data.
    zarr_output_path : str
        Path where the output Zarr dataset will be saved.
    """
    # extract the attributes for the final dataset
    attrs = get_orb_attrs(data_dict)

    # get the adjusted data_block
    osv_converted_to_dict = get_aux_orb_data_block(data_dict)

    # build the dataset
    position_keys = ["x", "y", "z"]
    velocity_keys = ["vx", "vy", "vz"]
    time_keys = ["tai", "utc", "ut1"]  # remove TAI=, UTC=, UT1=
    coords: dict[str, tuple[str, object] | object] = {
        "axis": np.array(["X", "Y", "Z"], dtype=object),
    }
    coords.update({k: ("tai", osv_converted_to_dict[k]) for k in time_keys})
    pos_da = xr.DataArray(
        data=[osv_converted_to_dict[k] for k in position_keys],
        dims=["axis", "tai"],
        coords=coords,
        attrs=ATTRS_POSITION,
    )
    vel_da = xr.DataArray(
        data=[osv_converted_to_dict[k] for k in velocity_keys],
        dims=["axis", "tai"],
        coords=coords,
        attrs=ATTRS_VELOCITY,
    )
    abs_orb_da = xr.DataArray(
        data=osv_converted_to_dict["absolute_orbit"],
        dims=["tai"],
        attrs=ATTRS_ABS_ORBIT,
    ).astype(np.int32)
    quality_da = xr.DataArray(
        data=_orb_quality_array(osv_converted_to_dict["quality"]),
        dims=["tai"],
        attrs=ATTRS_QUALITY,
    )
    data_dict = {
        "position": pos_da,
        "velocity": vel_da,
        "absolute_orbit": abs_orb_da,
        "quality": quality_da,
    }
    orb_ds = xr.Dataset(data_dict, attrs=attrs)
    return orb_ds


def save_aux_orb_xml_to_zarr(input_xml: str, zarr_output_path: str) -> None:
    """Parse an AUX_ORB XML file and save its contents to a Zarr store.

    This function performs the following steps:
    - Parses the AUX_ORB XML file into a Python dictionary.
    - Converts the dictionary into an xarray Dataset.
    - Saves the Dataset to disk in Zarr format.

    Parameters
    ----------
    input_xml : str
        Path to the AUX_ORB XML input file.
    zarr_output_path : str
        Destination directory path where the Zarr store will be saved.
    """
    # parse the xml file to a dict
    data_dict = convert_adf_xml_to_dict(input_xml)
    # convert to dataset
    orb_ds = convert_aux_orb_dict_to_dataset(data_dict)
    # save the dataset as a .zarr
    orb_ds.to_zarr(
        store=zarr_output_path,
        mode="a",
        consolidated=False,
        zarr_format=3,
    )
    LOGGER.info(f"File saved in {zarr_output_path}")


#####################
#                   #
#  ACCESSING UTILS  #
#                   #
#####################


def load_json_as_dict(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def findall_paths(struct, tag, path="", out=()):
    out = list(out)
    if isinstance(struct, dict):
        for k, v in struct.items():
            new_path = os.path.join(path, k)
            if tag in k:
                out.append(os.path.join(path, k))
            out.extend(findall_paths(v, tag, path=new_path))
    if isinstance(struct, list):
        for k, v in enumerate(struct):
            new_path = os.path.join(path, str(k))
            out.extend(findall_paths(v, tag, path=new_path))
    return out
