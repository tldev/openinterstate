# Release Format

This document defines the intended structure of an OpenInterstate public release.

## Release Cadence

Target starting cadence:

1. monthly full releases
2. ad hoc patch releases when schema or data quality requires it

## Release Directory Layout

```text
release-YYYY-MM-DD/
  manifest.json
  checksums.txt
  parquet/
    corridors.parquet
    corridor_edges.parquet
    corridor_exits.parquet
    exit_aliases.parquet
    places.parquet
    exit_place_links.parquet
    exit_place_scores.parquet
    reference_routes.parquet
  gpx/
    reference_routes.gpx.zip
  examples/
    sample_corridor.geojson
```

## Required Release Files

1. `manifest.json`
2. `checksums.txt`
3. at least one primary-format dataset bundle
4. attribution and lineage metadata

## Manifest Requirements

The release manifest must include:

1. release identifier
2. release date
3. schema version
4. source lineage summary
5. file inventory with sizes and checksums
6. attribution text or attribution reference

## Primary And Secondary Formats

Primary format:

- GeoParquet

Secondary formats:

- FlatGeobuf
- GPX for reference routes
- GeoJSON only for samples and small extracts

## Public Contract Rule

The release manifest and schema version together define the public release
contract. Pike-specific runtime exports are not part of this contract.
