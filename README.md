# OpenInterstate

Open interstate system data for the United States.

OpenInterstate is a public data project focused on turning raw
OpenStreetMap-derived interstate data into usable datasets:

- directional corridors
- normalized exits
- linked places and reachability metadata
- reference routes for exploration and QA

## Status

This repository is in bootstrap phase.

Current priorities:

1. define the public data contract
2. lock the naming system
3. document licensing and attribution requirements
4. stand up release and contribution scaffolding

## Project Boundary

OpenInterstate is intended to be the upstream data layer.

It does not define:

1. Pike app packaging
2. Pike runtime response contracts
3. Pike-specific SQLite export structure

## Start Here

- [Project charter](docs/charter.md)
- [Naming glossary](docs/naming_glossary.md)
- [Data contract draft](docs/data_contract_draft.md)
- [Licensing and attribution notes](docs/licensing_and_attribution.md)
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
