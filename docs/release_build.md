# Release Build Guide

This document describes the standalone OpenInterstate path for rebuilding a
public release from the raw U.S. OSM PBF.

## What This Flow Does

1. optionally prefilters the raw U.S. PBF into a canonical OpenInterstate import file
2. imports canonical OSM plus supporting road and POI context into local PostGIS with `osm2pgsql`
3. derives product tables with `schema/derive.sql`
4. builds graph, corridors, and reference routes with `openinterstate-derive`
5. exports a dated public release with manifest, checksums, and source lineage
6. optionally publishes the release to GitHub

The canonical database is intentionally broader than the final public release.
It retains context roads and POIs needed for exit and place derivation, while
the export step narrows the public release back to Interstate outputs.

## Prerequisites

Required on the host for local builds:

1. Docker

Optional on the host:

1. `gh` if you want to publish a release to GitHub from your machine

If local disk is constrained, put the managed data workspace on another volume:

```bash
./bin/openinterstate --data-dir /Volumes/goose-drive/openinterstate-data build
```

If you want release artifacts in a separate directory, set an explicit release
root:

```bash
./bin/openinterstate \
  --data-dir /Volumes/goose-drive/openinterstate-data \
  --release-dir /Volumes/goose-drive/openinterstate-releases \
  build
```

## Environment Setup

The default local workflow works without any env file and stores working data in
repo-local `.data/`, with release artifacts written to `.data/releases/`.

If you want to override the defaults, copy `.env.example` to `.env` and update:

1. the exposed Postgres host port
2. the managed data workspace root
3. the release output root
4. the default Geofabrik source URL
5. canonical import safety flags

## One-Command Build

```bash
./bin/openinterstate build
```

## Split Commands

If you want to run the pipeline in stages:

```bash
./bin/openinterstate download

./bin/openinterstate import \
  --pbf-file /abs/path/us-latest.osm.pbf \
  --force-prefilter

./bin/openinterstate derive

./bin/openinterstate release \
  --release-id release-$(date +%F)-local \
  --source-pbf-file /abs/path/us-latest.osm.pbf \
  --import-pbf-file /abs/path/us-latest.canonical-filtered.osm.pbf \
  --source-url https://download.geofabrik.de/north-america/us-latest.osm.pbf

./bin/openinterstate publish \
  --release-id release-$(date +%F)-local
```

If the database is already up to date, you can rerun just the `release` or
`publish` steps without re-importing the source PBF.

## Current v1 Outputs

1. `csv/corridors.csv`
2. `csv/corridor_edges.csv`
3. `csv/corridor_exits.csv`
4. `csv/exit_aliases.csv`
5. `csv/places.csv`
6. `csv/exit_place_links.csv`
7. `csv/exit_place_scores.csv`
8. `csv/reference_routes.csv`
9. `gpx/reference_routes.gpx.zip`
10. `examples/sample_corridors.geojson`
11. `manifest.json`
12. `source_lineage.json`
13. `checksums.txt`

Current export notes:

1. the public interstate filter includes plain numeric Interstates plus the
   currently supported official branch routes `I-35E`, `I-35W`, `I-69C`,
   `I-69E`, and `I-69W`
2. `exit_aliases.csv` is exported from `canonical_exit_aliases`, but is
   currently empty in the standalone pipeline
3. `reference_routes.csv` and `reference_routes.gpx.zip` only include routes
   that the derive step actually emits; the current builder skips corridors
   shorter than 50 km

## Geometry Representation In v1

The current public release writes CSV tables with geometry represented as
GeoJSON text columns. This is a pragmatic v1 choice; stricter Parquet or
GeoParquet packaging can follow later.

## Source Lineage

Every release now records:

1. the raw source PBF path, size, modified time, and SHA-256
2. the imported canonical filtered PBF path, size, modified time, and SHA-256
3. the source download URL when provided
4. the derivation chain used to produce the release

This lineage is published both inside `manifest.json` and as the standalone
asset `source_lineage.json`.

## Published Standalone Release

The current standalone release is published as:

1. GitHub release tag: `release-2026-03-12-goose-rerun-branchfix`
2. archive: `openinterstate-release-2026-03-12-goose-rerun-branchfix.tar.gz`
3. companion files: `manifest.json`, `source_lineage.json`, and `checksums.txt`
