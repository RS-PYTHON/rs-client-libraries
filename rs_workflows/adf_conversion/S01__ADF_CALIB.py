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

"""Convert local AUX_CAL SAFE products using the vendored Sentinel-1 helper."""

import argparse
import json
import logging
import os
import re
from pathlib import Path

import zarr
from aux_file_conversion import save_aux_cal_xml_to_zarr

SAFE_NAME = re.compile(
    r"S1(?P<platform>[A-D])_AUX_CAL_V(?P<validity>\d{8}T\d{6})_G(?P<generation>\d{8}T\d{6})\.SAFE",
    re.IGNORECASE,
)


def product_name(name: str) -> str:
    """Normalize staged item IDs and asset names to the SAFE product name."""
    for suffix in (".tar.gz", ".tgz", ".zip", ".tar"):
        if name.lower().endswith(suffix):
            name = name[: -len(suffix)]
            break
    return name.upper().removesuffix(".SAFE")


def convert_products(data_dir: Path, output_dir: Path, working_dir: Path) -> list[Path]:
    """Convert each extracted SAFE and retain the corresponding staged item's validity."""
    items = json.loads((working_dir / "aux_cal_items.json").read_text(encoding="utf-8"))
    source_items: dict[str, dict] = {}
    for item in items:
        names = [item["id"], *(Path(asset["href"]).name for asset in item["assets"].values())]
        for name in names:
            key = product_name(name)
            if SAFE_NAME.fullmatch(f"{key}.SAFE") is None:
                continue
            if key in source_items and source_items[key] != item:
                raise ValueError(f"Ambiguous staged AUX_CAL metadata for {name}")
            source_items[key] = item

    xml_files = sorted(data_dir.rglob("*-aux-cal.xml"))
    if not xml_files:
        raise ValueError(f"No AUX_CAL data XML files found in {data_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    products: list[Path] = []
    for xml_path in xml_files:
        safe_dir = xml_path.parent.parent
        match = SAFE_NAME.fullmatch(safe_dir.name)
        if match is None:
            raise ValueError(f"Invalid AUX_CAL SAFE product name: {safe_dir.name}")
        source = source_items.get(product_name(safe_dir.name))
        if source is None:
            raise ValueError(f"No staged AUX_CAL metadata for {safe_dir.name}")
        properties = source["properties"]
        if not properties.get("start_datetime") or not properties.get("end_datetime"):
            raise ValueError(f"Staged AUX_CAL item {source['id']} must have start_datetime and end_datetime")

        item_id = f"S01{match['platform'].upper()}_ADF_CALIB" f"_V{match['validity']}_G{match['generation']}"
        output_path = output_dir / f"{item_id}.zarr"
        if output_path.exists():
            raise FileExistsError(f"Duplicate AUX_CAL output: {output_path}")
        save_aux_cal_xml_to_zarr(str(xml_path), str(output_path))
        zarr.open_group(str(output_path), mode="a").attrs.update(
            {
                "id": item_id,
                "properties": {
                    "product:type": "ADF_CALIB",
                    "platform": f"sentinel-1{match['platform'].lower()}",
                    "start_datetime": properties["start_datetime"],
                    "end_datetime": properties["end_datetime"],
                    **({"created": properties["created"]} if properties.get("created") else {}),
                },
            },
        )
        products.append(output_path)
    return products


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--working_dir", type=Path, required=True)
    args = parser.parse_args()
    convert_products(args.data_dir, Path(os.environ["ADF_OUTPUT"]), args.working_dir)
