# OpenInterstate

OpenInterstate turns raw OpenStreetMap interstate data into reusable public
datasets for the United States.

The repo is organized around one job:

1. pull source data
2. import canonical OSM plus supporting road and POI context into local PostGIS
3. derive corridors, exits, places, and reference routes from that canonical store
4. export a dated interstate-focused public release with lineage

## One Command Local Run

If Docker is installed, this works from a fresh clone:

```bash
./bin/openinterstate --data-parent /Volumes/goose-drive/openinterstate build
```

That command downloads `us-latest.osm.pbf`, starts PostGIS, imports canonical
OSM, derives product tables, and writes a release under a workspace chosen from
the source PBF SHA-256:

```text
/Volumes/goose-drive/openinterstate/workspaces/pbf-sha256/<sha256>
```

Raw source downloads are shared under
`/Volumes/goose-drive/openinterstate/source-cache/`, and Cargo cache is shared
under `/Volumes/goose-drive/openinterstate/cache/cargo/` so Rust builds are
reused across PBF workspaces.

If you want release artifacts in a separate folder, set an explicit release
root:

```bash
./bin/openinterstate \
  --data-parent /Volumes/goose-drive/openinterstate \
  --release-dir /Volumes/goose-drive/openinterstate/releases \
  build
```

If you need to pin an exact workspace path and bypass the SHA-derived layout,
use `--data-dir` as an explicit override.

When the source PBF, import mapping, derive inputs, and release exporter are
unchanged, repeated builds now skip the already-current stages instead of
re-downloading or rebuilding them.

Fresh builds are faster too: the canonical prefilter/import now keeps only the
motorway/trunk road context and POI data needed for Interstate derivation, and
the downstream Rust graph builders stay focused on Interstate-labeled corridors
instead of constructing a much broader national highway graph.

## GitHub Actions Release Build

The repo now carries a manual GitHub Actions release workflow at
`.github/workflows/release-build.yml`.

That workflow is shaped to fit standard public GitHub-hosted runners:

1. download the raw `us-latest.osm.pbf` into short-lived runner storage
2. upload only the filtered `~160 MB` import PBF plus source metadata
3. rebuild PostGIS, derive tables, and export the release from that artifact
4. optionally publish the archive, manifest, checksums, and source lineage to GitHub

The raw source PBF is deleted after filtering and is never published as an
artifact, so the persisted handoff between jobs stays small even though the
prefilter job uses temporary local disk.

The manual `workflow_dispatch` path targets the full U.S. source file. The
`pull_request` path uses a smaller Rhode Island smoke-test extract so PR checks
validate the workflow mechanics without paying the full release-build cost on
every iteration.

## Repo Map

- `bin/`: the local command-line entrypoint
- `compose.yaml`: Docker services for PostGIS and the build runner
- `docker/runner/`: tool image with Rust, osm2pgsql, osmium, and Python
- `schema/`: bootstrap SQL, derive SQL, and the osm2pgsql flex mapping
- `tooling/`: release export and CI release scripts
- `crates/core/`: shared Rust geometry and highway helpers
- `crates/derive/`: Rust builders for graph, corridors, and reference routes
- `docs/`: project scope, contract, roadmap, and release docs
- `schemas/`: public manifest schemas
- `examples/`: example consumer queries

## Start Here

- [Release build guide](docs/release_build.md)
- [Project charter](docs/charter.md)
- [Data contract draft](docs/data_contract_draft.md)
- [Release format](docs/release_format.md)
- [Roadmap](docs/roadmap.md)
- [Program status](docs/program_status.md)
- [Licensing and attribution notes](docs/licensing_and_attribution.md)
- [Contributing](CONTRIBUTING.md)

## Project Boundary

OpenInterstate is the upstream interstate data layer. It does not define:

1. consumer app packaging
2. runtime response contracts for a specific client
3. application-specific delivery formats

## Public Surface

The current public release contains:

1. corridors
2. corridor edges
3. corridor exits
4. exit aliases
5. places
6. exit-place links
7. exit-place scores
8. reference routes

The internal canonical database is broader than the public release. It keeps
supporting highway context and POIs needed for derivation, but the exported
release is narrowed back to Interstate corridors and official signed branch
routes such as `I-35E`, `I-35W`, `I-69C`, `I-69E`, and `I-69W`.

`exit_aliases` is part of the public surface, but it is currently emitted as an
empty table until the standalone exit-alias normalization layer is populated.

Project links:

1. main repo: `https://github.com/tldev/openinterstate`
2. website repo: `https://github.com/tldev/openinterstate.org`
3. public site: `https://openinterstate.org`
