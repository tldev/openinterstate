# Data Contract Draft

This document defines the draft public surface for the first OpenInterstate
release.

## Goals

1. define stable public nouns
2. define a small, legible first release
3. separate public schema from application-specific runtime exports

## Draft Tables

### corridors

Represents contiguous directional interstate corridors.

Draft key fields:

- `corridor_id`
- `interstate_name`
- `direction_code`
- `direction_label`
- `distance_m`
- `geometry`

### corridor_edges

Represents graph edges that belong to corridors.

Draft key fields:

- `edge_id`
- `corridor_id`
- `sequence_index`
- `geometry`

### corridor_exits

Represents normalized exits attached to a corridor.

Draft key fields:

- `exit_id`
- `corridor_id`
- `exit_number`
- `exit_name`
- `lat`
- `lon`
- `geometry`

### exit_aliases

Maps source exits onto normalized corridor exits.

Draft key fields:

- `alias_id`
- `exit_id`
- `source_name`
- `source_ref`

### places

Represents places reachable from exits.

Draft key fields:

- `place_id`
- `name`
- `category`
- `brand`
- `lat`
- `lon`
- `geometry`

### exit_place_links

Represents link relationships between exits and places.

Draft key fields:

- `link_id`
- `exit_id`
- `place_id`
- `selection_state`

### exit_place_scores

Represents reachability and ranking metadata for a link.

Draft key fields:

- `link_id`
- `drive_time_s`
- `drive_distance_m`
- `provider`
- `score_version`

### reference_routes

Represents route artifacts used for exploration and QA.

Draft key fields:

- `reference_route_id`
- `corridor_id`
- `display_name`
- `distance_m`
- `duration_s`
- `geometry`

## Stable ID Rules

1. IDs must be deterministic.
2. IDs must survive table reorderings.
3. Public IDs must not depend on SQLite rowids or similar runtime details.

## Initial Release Formats

Primary:

- GeoParquet

Secondary:

- FlatGeobuf for GIS downloads
- GPX for reference routes
- JSON manifests for release metadata

## Explicit Non-Goals

This contract does not define:

1. consumer app responses
2. application-specific SQLite layouts
3. client-specific compatibility schemas
