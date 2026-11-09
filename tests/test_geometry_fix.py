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

"""Unit tests for rs_common/geometry_fix.py"""

from datetime import datetime, timezone

import pytest
from pystac import Item
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
    mapping,
    shape,
)

from rs_common.geometry_fix import fix_geojson_geometry, reduce_geometry_collection

# S3 OLCI L0 footprint (S03OLCL0__20261109T121139_0056_A080_T9E5.zarr) whose last vertex is duplicated with a
# 2e-14 longitude difference: make_valid turns it into GeometryCollection(Polygon, LineString).
OLCI_L0_SPIKE_FOOTPRINT = [
    (-125.7084295629045, -81.4022999452343),
    (-127.73153892413468, -81.38219485516419),
    (-129.74304169127242, -81.35178245614125),
    (-131.73839775773195, -81.31117089245261),
    (-133.7132910793247, -81.2605022366093),
    (-135.6636819778007, -81.1999500686356),
    (-137.58585022967148, -81.12971662247237),
    (-139.47642788434035, -81.0500296241875),
    (-141.33242283062077, -80.96113887772788),
    (-143.15123147915898, -80.86331277101),
    (-144.93064279304537, -80.75683473611933),
    (-144.92466349235107, -80.7544116888861),
    (-143.14564772965574, -80.86086171779006),
    (-141.32726480484197, -80.95866151032922),
    (-139.47172492325765, -81.04752784538157),
    (-137.5816303710573, -81.12719254231871),
    (-135.65997146611124, -81.1974059966458),
    (-133.71011387194366, -81.25794066953736),
    (-131.73577504496623, -81.30859449764121),
    (-129.7409914483601, -81.34919405055386),
    (-127.73007551389233, -81.37959738042075),
    (-125.70756340719598, -81.39969643916012),
    (-123.67815582543275, -81.40941895899104),
    (-123.67841849043103, -81.41202539359001),
    (-123.67841849043101, -81.41202539359001),
    (-123.67841849043103, -81.41202539359001),
    (-125.7084295629045, -81.4022999452343),
]


def _item(geometry: dict) -> Item:
    return Item(
        id="test-item",
        geometry=geometry,
        bbox=list(shape(geometry).bounds),
        datetime=datetime(2026, 11, 9, tzinfo=timezone.utc),
        properties={},
    )


def test_fix_geojson_geometry_reduces_geometry_collection_created_by_make_valid():
    """A polygon that make_valid turns into a GeometryCollection is published as a valid Polygon."""
    item = _item({"type": "Polygon", "coordinates": [OLCI_L0_SPIKE_FOOTPRINT]})
    fix_geojson_geometry(item)

    fixed = shape(item.geometry)
    assert item.geometry["type"] == "Polygon"
    assert fixed.is_valid
    assert fixed.exterior.is_ccw
    assert fixed.area == pytest.approx(Polygon(OLCI_L0_SPIKE_FOOTPRINT).buffer(0).area)
    assert tuple(item.bbox) == pytest.approx(fixed.bounds)


def test_fix_geojson_geometry_keeps_input_geometry_collection():
    """A valid GeometryCollection provided by the processor is kept as is."""
    collection = GeometryCollection(
        [Polygon([(0, 0), (0, 1), (1, 1), (1, 0)]).reverse(), LineString([(2, 2), (3, 3)]), Point(5, 5)],
    )
    item = _item(mapping(collection))
    fix_geojson_geometry(item)

    assert shape(item.geometry).equals(collection)
    assert item.geometry["type"] == "GeometryCollection"
    assert len(item.geometry["geometries"]) == 3
    assert tuple(item.bbox) == pytest.approx(collection.bounds)


@pytest.mark.parametrize(
    "geometry, expected_type",
    [
        (GeometryCollection([Polygon([(0, 0), (1, 0), (1, 1)]), Point(5, 5)]), Polygon),
        (
            GeometryCollection([Polygon([(0, 0), (1, 0), (1, 1)]), Polygon([(3, 0), (4, 0), (4, 1)])]),
            MultiPolygon,
        ),
        (GeometryCollection([LineString([(0, 0), (1, 1)]), Point(5, 5)]), LineString),
        (GeometryCollection([LineString([(0, 0), (1, 1)]), LineString([(3, 3), (4, 4)])]), MultiLineString),
        (GeometryCollection([Point(0, 0)]), Point),
        (GeometryCollection([Point(0, 0), Point(1, 1)]), MultiPoint),
        (GeometryCollection(), GeometryCollection),
        (Polygon([(0, 0), (1, 0), (1, 1)]), Polygon),
    ],
)
def test_reduce_geometry_collection(geometry, expected_type):
    """Only the highest-dimension parts of a GeometryCollection are kept, other geometries are unchanged."""
    assert type(reduce_geometry_collection(geometry)) is expected_type  # pylint: disable=unidiomatic-typecheck
