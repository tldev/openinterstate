# Data Contract Draft

This document defines the draft public surface for the first OpenInterstate
release.

## Goals

1. define stable public nouns
2. define a small, legible first release
3. separate public schema from application-specific runtime exports

Current v1 packaging is CSV-first. Geometry columns are emitted as GeoJSON text
inside the CSV tables, with GPX used for reference-route exports.

## Draft Tables

### corridors

Represents contiguous directional interstate corridors.

Draft key fields:

- `corridor_id`
- `interstate_name`
- `direction_code`
- `direction_label`
- `geometry_geojson`
- `edge_count`

### corridor_edges

Represents graph edges that belong to corridors.

`direction_code` is the corridor's canonical direction. It is not the raw
travel direction computed for an individual internal `highway_edges` row.

Draft key fields:

- `edge_id`
- `corridor_id`
- `interstate_name`
- `direction_code`
- `length_m`
- `geometry_geojson`

Internal note: `highway_edges.direction` remains derive-stage metadata, but it
is not part of corridor membership semantics or the public corridor edge
contract.

### corridor_exits

Represents normalized exits attached to a corridor.

Draft key fields:

- `exit_id`
- `corridor_id`
- `interstate_name`
- `direction_code`
- `sequence_index`
- `exit_number`
- `exit_name`
- `lat`
- `lon`
- `geometry_geojson`

### exit_aliases

Maps source exits onto normalized corridor exits.

Current standalone status: this table is exported as a two-column mapping from
`canonical_exit_id` to `source_exit_id`, but it is currently empty because the
standalone alias-normalization layer is not yet populated.

Draft key fields:

- `canonical_exit_id`
- `source_exit_id`

### places

Represents places reachable from exits.

Draft key fields:

- `place_id`
- `category`
- `name`
- `display_name`
- `brand`
- `geometry_geojson`

### exit_place_links

Represents link relationships between exits and places.

Draft key fields:

- `exit_id`
- `place_id`
- `category`
- `distance_m`
- `rank`

### exit_place_scores

Represents reachability and ranking metadata for a link.

Draft key fields:

- `exit_id`
- `place_id`
- `route_distance_m`
- `route_duration_s`
- `reachable`
- `reachability_score`
- `reachability_confidence`
- `provider`
- `provider_dataset_version`
- `updated_at`

### reference_routes

Represents route artifacts used for exploration and QA.

Current standalone status: only corridors that meet the current route-builder
thresholds are emitted as reference routes. Short corridors may still appear in
`corridors.csv` without a matching reference route.

Draft key fields:

- `reference_route_id`
- `interstate_name`
- `direction_code`
- `direction_label`
- `display_name`
- `distance_m`
- `duration_s`
- `point_count`
- `waypoints_json`

## Stable ID Rules

1. IDs must be deterministic.
2. IDs must survive table reorderings.
3. Public IDs must not depend on SQLite rowids or similar runtime details.

## Initial Release Formats

Primary:

- CSV with GeoJSON text geometry columns

Secondary:

- GPX for reference routes
- GeoJSON example extracts
- JSON manifests for release metadata

Planned follow-on formats:

- Parquet / GeoParquet
- FlatGeobuf

## Explicit Non-Goals

This contract does not define:

1. consumer app responses
2. application-specific SQLite layouts
3. client-specific compatibility schemas
