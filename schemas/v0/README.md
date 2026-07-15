# Schemas v0

This folder contains the machine-readable schema artifacts for OpenInterstate
public releases.

Current contents:

1. `manifest.schema.json` for release manifests

Table-level schemas live in `datapackage.json` at the repo root, following the
Data Package standard (https://datapackage.org). That file is the source of
truth for the data model documentation on openinterstate.org, which syncs it at
build time.
