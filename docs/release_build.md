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

The default local layout uses a parent data root and derives a per-PBF
workspace from the source file SHA-256:

```bash
./bin/openinterstate --data-parent /Volumes/goose-drive/openinterstate build
```

That resolves the working workspace to:

```text
/Volumes/goose-drive/openinterstate/workspaces/pbf-sha256/<sha256>
```

Shared raw source downloads live under
`/Volumes/goose-drive/openinterstate/source-cache/`, and shared Cargo cache
lives under `/Volumes/goose-drive/openinterstate/cache/cargo/`.

If you want release artifacts in a separate directory, set an explicit release root:

```bash
./bin/openinterstate \
  --data-parent /Volumes/goose-drive/openinterstate \
  --release-dir /Volumes/goose-drive/openinterstate/releases \
  build
```

If you need to bypass the SHA-derived layout, use `--data-dir` to pin an exact
workspace path explicitly.

## GitHub Actions Workflow

The repo also includes a manual GitHub Actions workflow at
`.github/workflows/release-build.yml`.

That workflow is designed for standard public GitHub-hosted runners:

1. download the raw U.S. PBF into temporary runner storage
2. persist only the filtered canonical import PBF plus source metadata as an artifact
3. rebuild PostGIS, derive product tables, and export the release from that filtered artifact
4. optionally publish the archive and companion metadata files to GitHub Releases

The raw source file is deleted after filtering and is never passed between jobs
as an artifact. The only persisted handoff is the filtered canonical import PBF
plus its source metadata.

The manual `workflow_dispatch` run uses the full U.S. source by default. The
`pull_request` trigger is intentionally lighter and uses a Rhode Island smoke
test extract so release-workflow changes can be validated quickly in PRs.

## Environment Setup

The default local workflow works without any env file and uses the goose-drive
parent root `/Volumes/goose-drive/openinterstate`. Each source PBF resolves to
its own workspace under `workspaces/pbf-sha256/<sha256>`.

If you want to override the defaults, copy `.env.example` to `.env` and update:

1. the exposed Postgres host port
2. the managed parent data root
3. the optional explicit workspace or release output root
4. the default Geofabrik source URL
5. canonical import safety flags

## One-Command Build

```bash
./bin/openinterstate --data-parent /Volumes/goose-drive/openinterstate build
```

## Publish Step

After a successful build, publish the generated release to GitHub:

```bash
./bin/openinterstate publish \
  --pbf-url https://download.geofabrik.de/north-america/us-latest.osm.pbf \
  --release-id release-$(date +%F)
```

The main `build` command now also skips current download, filter, import,
derive, and release stages when their inputs and build scripts have not
changed.

## Current v1 Outputs

1. `csv/corridors.csv`
2. `csv/corridor_edges.csv`
3. `csv/corridor_exits.csv`
4. `csv/exit_aliases.csv`
5. `csv/places.csv`
6. `csv/exit_place_links.csv`
7. `csv/reference_routes.csv`
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

1. the raw source PBF path or streamed source URL, plus size, modified time, and SHA-256
2. the imported canonical filtered PBF path, size, modified time, and SHA-256
3. the source download URL when provided
4. the derivation chain used to produce the release

This lineage is published both inside `manifest.json` and as the standalone
asset `source_lineage.json`.

When the GitHub Actions workflow streams the raw source instead of storing it on
disk, `source_pbf.path` is recorded as the downloaded source URL.

## Published Standalone Release

The current standalone release is published as:

1. GitHub release tag: `release-2026-03-12-coldpath`
2. archive: `openinterstate-release-2026-03-12-coldpath.tar.gz`
3. companion files: `manifest.json`, `source_lineage.json`, and `checksums.txt`
