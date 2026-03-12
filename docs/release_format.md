# Release Format

This document defines the intended structure of an OpenInterstate public release.

Current v1 scope notes:

1. the public release exports Interstate records only, including the supported
   official branch routes `I-35E`, `I-35W`, `I-69C`, `I-69E`, and `I-69W`
2. arbitrary `I-*` labels that are not treated as public Interstate route names
   are excluded from the release
3. `exit_aliases.csv` is part of the release layout, but may be empty until the
   standalone alias-normalization layer is populated
4. `reference_routes.csv` and `reference_routes.gpx.zip` are generated only for
   corridors that meet the current route-builder thresholds

## Release Cadence

Target starting cadence:

1. monthly full releases
2. ad hoc patch releases when schema or data quality requires it

## Release Directory Layout

```text
release-YYYY-MM-DD/
  manifest.json
  source_lineage.json
  checksums.txt
  csv/
    corridors.csv
    corridor_edges.csv
    corridor_exits.csv
    exit_aliases.csv
    places.csv
    exit_place_links.csv
    exit_place_scores.csv
    reference_routes.csv
  gpx/
    reference_routes.gpx.zip
  examples/
    sample_corridors.geojson
```

## Required Release Files

1. `manifest.json`
2. `source_lineage.json`
3. `checksums.txt`
4. at least one primary-format dataset bundle
5. attribution and lineage metadata

## Manifest Requirements

The release manifest must include:

1. release identifier
2. release date
3. schema version
4. source lineage object for both the raw source PBF and the imported canonical PBF
5. file inventory with sizes and checksums
6. attribution text or attribution reference

## Primary And Secondary Formats

Current v1 release formats:

- CSV for primary tabular datasets
- GPX for reference routes
- GeoJSON for small example extracts

Planned follow-on formats:

- Parquet / GeoParquet
- FlatGeobuf

## Public Contract Rule

The release manifest and schema version together define the public release
contract. Application-specific runtime exports are not part of this contract.
