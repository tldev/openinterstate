# OpenInterstate

Open interstate system data for the United States.

OpenInterstate is a public data project focused on turning raw
OpenStreetMap-derived interstate data into usable datasets:

- directional corridors
- normalized exits
- linked places and reachability metadata
- reference routes for exploration and QA

## Status

The standalone OpenInterstate pipeline can now rebuild the public dataset from
the raw U.S. OSM PBF and publish a release with explicit source lineage.

Current priorities:

1. finalize licensing and attribution posture for long-term operation
2. turn the API contract into a running public service
3. improve packaging beyond the pragmatic CSV-first v1 release
4. keep future releases aligned with the published release contract

## Project Boundary

OpenInterstate is intended to be the upstream data layer.

It does not define:

1. consumer app packaging
2. client-specific runtime response contracts
3. application-specific delivery formats

## Start Here

- [Project charter](docs/charter.md)
- [Naming glossary](docs/naming_glossary.md)
- [Data contract draft](docs/data_contract_draft.md)
- [Release format](docs/release_format.md)
- [Roadmap](docs/roadmap.md)
- [Program status](docs/program_status.md)
- [Licensing and attribution notes](docs/licensing_and_attribution.md)
- [Release build guide](docs/release_build.md)
- [Contributing](CONTRIBUTING.md)

## Initial Scope

The initial public surface is expected to cover:

1. corridors
2. corridor edges
3. corridor exits
4. exit aliases
5. places
6. exit-place links
7. exit-place scores
8. reference routes

## Working Principles

1. Prefer stable public names over internal implementation names.
2. Publish reproducible releases before committing to a public API.
3. Keep the public data contract decoupled from any one downstream client.
4. Treat licensing and attribution as first-class requirements.
5. Prefer real release artifacts over mocked examples whenever feasible.

## Public Surfaces

1. Main project repo: `https://github.com/tldev/openinterstate`
2. Website repo: `https://github.com/tldev/openinterstate.org`
3. Public site: `https://openinterstate.org`
