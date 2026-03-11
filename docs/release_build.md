# Release Build Guide

This document describes the standalone OpenInterstate path for rebuilding a
public release from the raw U.S. OSM PBF.

## What This Flow Does

1. optionally prefilters the raw U.S. PBF into a canonical OpenInterstate import file
2. imports canonical OSM into local PostGIS with `osm2pgsql`
3. derives product tables with `pipeline/sql/derive_product.sql`
4. builds graph, corridors, and reference routes with `openinterstate-import`
5. exports a dated public release with manifest, checksums, and source lineage
6. optionally publishes the release to GitHub

## Prerequisites

Required on the host:

1. Docker
2. `osm2pgsql`
3. `osmium`
4. `python3`
5. `gh` for publishing

## Environment Setup

Copy `.env.pipeline.example` to `.env.pipeline` and set the host paths for:

1. PostGIS data directory
2. flatnodes cache
3. raw PBF download directory
4. filtered canonical PBF directory
5. build output directory

The host-accessible DB fields `OSM_DB_*` are used for export. The
container-network `PRODUCT_DB_URL` is used by the Rust steps run through Docker.

## One-Command Build

```bash
./openinterstate-pipeline.sh --env-file .env.pipeline run \
  --pbf-file /abs/path/us-latest.osm.pbf \
  --source-url https://download.geofabrik.de/north-america/us-latest.osm.pbf \
  --release-id release-2026-03-11-standalone \
  --force-prefilter
```

## Split Commands

If you want to run the pipeline in stages:

```bash
./openinterstate-pipeline.sh --env-file .env.pipeline import-canonical \
  --pbf-file /abs/path/us-latest.osm.pbf \
  --force-prefilter

./openinterstate-pipeline.sh --env-file .env.pipeline derive

./openinterstate-pipeline.sh --env-file .env.pipeline release \
  --release-id release-2026-03-11-standalone \
  --source-pbf-file /abs/path/us-latest.osm.pbf \
  --import-pbf-file /abs/path/us-latest.canonical-filtered.osm.pbf \
  --source-url https://download.geofabrik.de/north-america/us-latest.osm.pbf

./openinterstate-pipeline.sh --env-file .env.pipeline publish \
  --release-id release-2026-03-11-standalone
```

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

1. GitHub release tag: `release-2026-03-11-standalone`
2. archive: `openinterstate-release-2026-03-11-standalone.tar.gz`
3. companion files: `manifest.json`, `source_lineage.json`, and `checksums.txt`
