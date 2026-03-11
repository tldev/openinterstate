# Release Build Guide

This document describes the current path for building the first public
OpenInterstate release from a live Pike/Postgres dataset.

## Current Source Assumption

The current exporter reads from the Pike Postgres database and filters to a
strict interstate-only surface:

1. explicit `I-<number>` corridor names only
2. no business, truck, express, HOV, or connector variants in v1

## Current v1 Outputs

1. `corridors.parquet`
2. `corridor_edges.csv`
3. `corridor_exits.csv`
4. `exit_aliases.csv`
5. `places.csv`
6. `exit_place_links.csv`
7. `exit_place_scores.csv`
8. `reference_routes.csv`
9. `reference_routes.gpx.zip`
10. `manifest.json`
11. `checksums.txt`

## Geometry Representation In v1

The first public release writes CSV tables with geometry represented as GeoJSON
text columns.

This is a pragmatic v1 decision to get a real release out quickly from the
existing Pike source tables. A stricter Parquet or GeoParquet contract can
follow in a later release.

## Build Command

```bash
python3 scripts/export_v1_release.py \
  --database-url postgres://osm:osm_dev@localhost:5433/osm \
  --release-id release-2026-03-11 \
  --output-dir build/release-2026-03-11
```

## Dependencies

Install:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-release.txt
```
