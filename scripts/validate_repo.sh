#!/usr/bin/env bash
set -euo pipefail

required_files=(
  "README.md"
  "CONTRIBUTING.md"
  "CODEOWNERS"
  "LICENSE"
  "DATA_LICENSE.md"
  "docs/charter.md"
  "docs/naming_glossary.md"
  "docs/data_contract_draft.md"
  "docs/licensing_and_attribution.md"
  "docs/release_format.md"
  "docs/release_build.md"
  "docs/roadmap.md"
  "docs/program_status.md"
  "docs/external_blockers.md"
  "api/openapi.yaml"
  "schemas/v0/manifest.schema.json"
  "examples/duckdb/example_queries.sql"
  ".env.pipeline.example"
  "docker-compose.yml"
  "openinterstate-pipeline.sh"
  "pipeline/bin/openinterstate-pipeline.sh"
  "pipeline/bin/import_canonical_osm.sh"
  "pipeline/bin/derive_product.sh"
  "pipeline/bin/export_release.sh"
  "pipeline/bin/publish_release.sh"
  "pipeline/bin/download_us_pbf.sh"
  "pipeline/lib/common.sh"
  "pipeline/sql/derive_product.sql"
  "config/osm2pgsql/openinterstate_v1.lua"
  "migrations/001_bootstrap.sql"
  "openinterstate-core/Cargo.toml"
  "openinterstate-import/Cargo.toml"
)

for file in "${required_files[@]}"; do
  test -f "$file"
done
