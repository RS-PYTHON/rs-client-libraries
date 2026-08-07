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

"""This module is used to share common functions between apis and to fix the malformed
generated products by a processor. It is not part of the Sonarqube coverage part, because
it is a temporary fix of the malformed generated products by a processor. The processor
should be fixed in the future, and this module should be removed.
"""

import logging
from typing import Any

import antimeridian
from geojson_pydantic.geometries import LineString as GeoLineString
from geojson_pydantic.geometries import MultiLineString as GeoMultiLineString
from geojson_pydantic.geometries import MultiPolygon as GeoMultiPolygon
from geojson_pydantic.geometries import Polygon as GeoPolygon
from prefect import get_run_logger
from prefect.exceptions import MissingContextError
from pystac import Item
from shapely import get_parts, make_valid, union_all
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
from shapely.geometry.polygon import orient

from rs_common.footprint_facility import (
    AlreadyReworkedPolygonError,
    check_cross_antimeridian,
    check_raw_antimeridian_jump,
    rework_to_linestring_geometry,
    rework_to_polygon_geometry,
)


def _get_logger():
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


# Map Shapely types to Pydantic GeoJSON types
shapely_to_geojson_cls = {
    LineString: GeoLineString,
    MultiLineString: GeoMultiLineString,
    Polygon: GeoPolygon,
    MultiPolygon: GeoMultiPolygon,
}


def looks_like_swath_polygon(geometry: Polygon) -> bool:
    """Return True for a polygon that can be rebuilt as a swath strip.

    Criteria:
    - no interior rings/holes;
    - at least four exterior points, excluding closure;
    - an even number of exterior points, so the ring can be split into two paired edges.
    """
    # Holes make the two-edge strip interpretation unsafe.
    if geometry.interiors:
        return False

    points = [(float(lon), float(lat)) for lon, lat in geometry.exterior.coords[:-1]]
    # A swath strip needs two paired edges; odd rings cannot be paired.
    if len(points) < 4 or len(points) % 2:
        return False

    return True


def rebuild_swath_polygon(geometry: Polygon) -> MultiPolygon | Polygon:
    """Rebuild a two-edge swath footprint into antimeridian-safe geometry.

    The polygon exterior is interpreted as two paired swath edges: the first half
    of the coordinates is one edge, and the reversed second half is the opposite
    edge. The footprint is rebuilt as consecutive quadrilateral cells between
    paired vertices. Each cell is fixed for +/-180 longitude crossings, then the
    fixed polygon parts are unioned back into a Polygon or MultiPolygon.

    References:
    - GeoJSON antimeridian cutting:
      https://www.rfc-editor.org/rfc/rfc7946#section-3.1.9
    - antimeridian segmentation algorithm:
      https://www.gadom.ski/antimeridian/v0.4.5/the-algorithm/
    - antimeridian.fix_shape API:
      https://www.gadom.ski/antimeridian/v0.4.5/api/#antimeridian.fix_shape
    """

    def polygon_parts(geometry):
        parts = []
        for part in get_parts(geometry):
            if isinstance(part, Polygon):
                parts.append(part)
            elif isinstance(part, (MultiPolygon, GeometryCollection)):
                parts.extend(polygon_parts(part))
        return parts

    logger = _get_logger()
    # Drop the closing coordinate and split the ring into the two swath borders.
    points = [(float(lon), float(lat)) for lon, lat in geometry.exterior.coords[:-1]]
    half = len(points) // 2
    edge_a = points[:half]
    edge_b = list(reversed(points[half:]))
    parts = []

    for index in range(half - 1):
        # Rebuild one local strip cell between paired vertices on both borders.
        quad = Polygon([edge_a[index], edge_a[index + 1], edge_b[index + 1], edge_b[index], edge_a[index]])
        try:
            # Fix the local cell at +/-180 without letting a global repair choose the wrong interior.
            fixed_quad = shape(antimeridian.fix_shape(quad, fix_winding=True, great_circle=False))
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.warning(f"antimeridian.fix_shape failed on swath quad, using local make_valid: {exception}")
            fixed_quad = quad
        if not fixed_quad.is_valid:
            fixed_quad = make_valid(fixed_quad)
        parts.extend(polygon_parts(fixed_quad))

    if not parts:
        return geometry

    # Merge repaired cells back into a single polygonal footprint.
    rebuilt = union_all(parts)
    if not rebuilt.is_valid:
        rebuilt = make_valid(rebuilt)
    polygons = polygon_parts(rebuilt)
    return MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]


def repair_and_orient_geojson_geometry(geometry: dict[str, Any]) -> dict[str, Any]:
    """Repair invalid GeoJSON if needed and enforce RFC7946 ring orientation on polygonal results."""
    shapely_geometry = shape(geometry)
    # For raw polygonal footprints crossing the antimeridian, run the footprint-facility rework before
    # generic make_valid. Shapely repairs lon/lat coordinates in a planar space; applying make_valid first
    # can split a self-intersecting antimeridian footprint into a MultiPolygon that footprint-facility later
    # treats as already reworked and may reject with AlreadyReworkedPolygonError. Use check_raw_antimeridian_jump
    # here, not check_cross_antimeridian: a longitude jump > 180 is the raw-data signal that a segment
    # should cross the antimeridian, while coordinates already placed on +/-180 are often the result of
    # a previous split/rework and must not trigger another early polygon rework.
    logger = _get_logger()
    if isinstance(shapely_geometry, Polygon) and check_raw_antimeridian_jump(shapely_geometry):
        if looks_like_swath_polygon(shapely_geometry):
            # Swath footprints are strips encoded as two borders; rebuild them as small cells so
            # antimeridian repair keeps the strip shape instead of guessing a large polygon interior.
            shapely_geometry = rebuild_swath_polygon(shapely_geometry)
        else:
            shapely_geometry = rework_to_polygon_geometry(shapely_geometry)
    elif isinstance(shapely_geometry, MultiPolygon) and check_raw_antimeridian_jump(shapely_geometry):
        shapely_geometry = rework_to_polygon_geometry(shapely_geometry)

    if not shapely_geometry.is_valid:
        shapely_geometry = make_valid(shapely_geometry)

    if isinstance(shapely_geometry, Polygon):
        # RFC7946 expects exterior rings CCW and interior rings CW.
        logger.info("Orienting polygon rings to RFC7946 standard.")
        return mapping(orient(shapely_geometry, sign=1.0))

    if isinstance(shapely_geometry, MultiPolygon):
        # Apply the same ring orientation rule to each polygon component independently.
        normalized = MultiPolygon([orient(polygon, sign=1.0) for polygon in shapely_geometry.geoms])
        logger.info("Orienting multipolygon rings to RFC7946 standard.")
        return mapping(normalized)

    # If make_valid returns another valid geometry type, keep it unchanged.
    return mapping(shapely_geometry)


def fix_geojson_geometry(item: Item):
    """Repair invalid GeoJSON if needed and enforce RFC7946 ring orientation on polygonal results.

    This function is a wrapper around repair_and_orient_geojson_geometry that logs any exceptions
    raised during the repair process. If an exception occurs, the original geometry is returned unchanged.
    """
    logger = _get_logger()
    # Skip if feature does not have geometry
    if not (geometry := item.geometry):
        logger.info("Item has no geometry, skipping fix_geojson_geometry.")
        return

    # Apply the general repair first; the remaining logic handles antimeridian-specific cases.
    geometry = repair_and_orient_geojson_geometry(geometry)
    item.geometry = geometry
    item.bbox = shape(geometry).bounds
    # Skip if geometry does not cross the antimeridian
    shapely_geom = shape(geometry)
    # Points do not define a path, so antimeridian crossing checks can be false positives for MultiPoint.
    if isinstance(shapely_geom, (Point, MultiPoint)):
        logger.info("Item has a point geometry, skipping fix_geojson_geometry.")
        return

    reworked_geometry = shapely_geom
    if isinstance(shapely_geom, (Polygon, MultiPolygon)):
        # Only retry polygon rework for invalid geometries that still look antimeridian-related.
        if shapely_geom.is_valid or not check_cross_antimeridian(shapely_geom):
            logger.info(
                "Item has a valid polygon geometry or does not cross the "
                "antimeridian, skipping fix_geojson_geometry.",
            )
            return
        # Rework geometry to be a valid Polygon / MultiPolygon
        try:
            reworked_geometry = rework_to_polygon_geometry(shapely_geom)
        except AlreadyReworkedPolygonError:
            if not isinstance(shapely_geom, MultiPolygon):
                logger.error(f"Failed to rework geometry: {shapely_geom}")
                raise
            # make_valid can leave an antimeridian footprint as a MultiPolygon that footprint-facility
            # considers already reworked; keep it instead of failing the whole items response.
            reworked_geometry = shapely_geom
    elif isinstance(shapely_geom, (LineString, MultiLineString)):
        # Rework only raw line crossings to avoid unnecessary processing and to keep already split
        # lines idempotent: they may touch +/-180 but should not be reworked again.
        if not check_raw_antimeridian_jump(shapely_geom):
            logger.info("Item has a line geometry that does not cross the antimeridian, skipping fix_geojson_geometry.")
            return
        # Rework geometry to be a valid LineString / MultiLineString
        reworked_geometry = rework_to_linestring_geometry(shapely_geom)

    geo_cls = shapely_to_geojson_cls.get(type(reworked_geometry))
    if not geo_cls:
        logger.error(f"Unsupported geometry type: {type(reworked_geometry)}")
        raise TypeError(f"Unsupported geometry type: {type(reworked_geometry)}")

    # Re-apply orientation after antimeridian rework because the split can yield MultiPolygons.
    normalized_geometry = repair_and_orient_geojson_geometry(mapping(reworked_geometry))
    # PySTAC expects a plain GeoJSON mapping, not a geojson-pydantic model.
    item.geometry = normalized_geometry
    # Keep bbox consistent with the reworked geometry.
    item.bbox = shape(normalized_geometry).bounds
    logger.info(f"Item geometry fixed and oriented: {item.geometry}")
