# OpenInterstate

OpenInterstate publishes open, versioned datasets of the United States
interstate highway system, derived from OpenStreetMap.

Getting usable interstate data out of raw OSM requires planet-file
processing, a spatial database, and a stack of conflation heuristics. This
project does that work once per release and ships the result as plain CSV, so
consumers can skip straight to the data.

## Get the data

Every release is a dated, checksummed tarball on the
[releases page](https://github.com/tldev/openinterstate/releases):

```bash
gh release download --repo tldev/openinterstate --pattern '*.tar.gz'
```

A release contains seven CSV tables plus GPX reference routes, GeoJSON
examples, a manifest, checksums, and source lineage:

| Table | Contents |
| --- | --- |
| `corridors` | Contiguous directional interstate corridors |
| `corridor_edges` | Graph edges assigned to corridors |
| `corridor_exits` | Normalized exits linked to a corridor |
| `exit_aliases` | Source exit aliases mapped to normalized exits (currently empty) |
| `places` | Reachable places and services |
| `exit_place_links` | Spatial proximity links between exits and places |
| `reference_routes` | Routes for QA, examples, and exploration |

Every table and column is documented in [datapackage.json](datapackage.json)
(Data Package standard) and rendered at <https://openinterstate.org/schema>.
Example DuckDB queries live in [examples/duckdb](examples/duckdb).

## Scope

Releases cover signed interstates only, including lettered branches such as
`I-35E` and `I-69C`. OpenInterstate is an upstream data layer: it does not
define consumer packaging or application-specific contracts. Drive-time
reachability scores are published separately by
[openinterstate-reachability](https://github.com/tldev/openinterstate-reachability).

## How it works

```
us-latest.osm.pbf (Geofabrik)
  -> osmium prefilter (interstate-relevant ways, exits, POIs)
  -> osm2pgsql flex import into PostGIS
  -> SQL and Rust derive stages (graph, corridors, exits, routes)
  -> CSV export with manifest, checksums, and lineage
```

Releases are built by the `Release Build` GitHub Actions workflow, triggered
manually with an opt-in publish step. Pull requests run the same mechanics
against a small Rhode Island extract as a smoke test.

## Build it yourself

Docker is the only requirement:

```bash
./bin/openinterstate --data-parent /path/to/openinterstate-data build
```

This downloads the source PBF, starts PostGIS, imports and derives, and
writes a release under a workspace keyed by the source file's SHA-256.
Re-runs skip stages whose inputs have not changed. Add `--release-dir <path>`
to collect release artifacts in one place.

## Repository layout

- `bin/` local command-line entrypoint
- `compose.yaml` Docker services for PostGIS and the build runner
- `docker/runner/` tool image with Rust, osm2pgsql, osmium, and Python
- `schema/` bootstrap SQL, derive SQL, and the osm2pgsql flex mapping
- `crates/` Rust builders for the graph, corridors, and reference routes
- `tooling/` release export and CI scripts
- `datapackage.json` machine-readable schema for every public table
- `schemas/` release manifest schema
- `examples/` example consumer queries

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Naming, schema, and release-format
review are the most useful contributions right now.

## License

Code is MIT ([LICENSE](LICENSE)). Data releases are OpenStreetMap-derived and
published under ODbL 1.0 ([DATA_LICENSE.md](DATA_LICENSE.md)).
