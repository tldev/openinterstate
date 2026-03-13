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
./bin/openinterstate build
```

That command downloads `us-latest.osm.pbf`, starts PostGIS, imports canonical
OSM, derives product tables, and writes a release into `.data/releases/` by
default.

If your main disk is tight, move the managed data workspace onto another volume:

```bash
./bin/openinterstate --data-dir /Volumes/goose-drive/openinterstate-data build
```

With that command, working data and release artifacts both land under
`/Volumes/goose-drive/openinterstate-data/`.

Runner caches now follow the managed data root too, so Cargo registry/git
state and the Rust target directory stay on the external volume instead of
quietly growing inside Docker-managed local storage.

If you want release artifacts in a separate folder, set an explicit release
root:

```bash
./bin/openinterstate \
  --data-dir /Volumes/goose-drive/openinterstate-data \
  --release-dir /Volumes/goose-drive/openinterstate-releases \
  build
```

When the source PBF, import mapping, derive inputs, and release exporter are
unchanged, repeated builds now skip the already-current stages instead of
re-downloading or rebuilding them.

Fresh builds are faster too: the canonical prefilter/import now keeps only the
motorway/trunk road context and POI data needed for Interstate derivation, and
the downstream Rust graph builders stay focused on Interstate-labeled corridors
instead of constructing a much broader national highway graph.

## Repo Map

- `bin/`: the local command-line entrypoint
- `compose.yaml`: Docker services for PostGIS and the build runner
- `docker/runner/`: tool image with Rust, osm2pgsql, osmium, and Python
- `schema/`: bootstrap SQL, derive SQL, and the osm2pgsql flex mapping
- `tooling/`: release export and repo validation scripts
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
