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
  ".env.example"
  "compose.yaml"
  "bin/openinterstate"
  "bin/lib.sh"
  "docker/runner/Dockerfile"
  "schema/bootstrap.sql"
  "schema/derive.sql"
  "schema/osm2pgsql/openinterstate.lua"
  "tooling/export_release.py"
  "tooling/requirements.txt"
  "tooling/validate_repo.sh"
  "crates/core/Cargo.toml"
  "crates/derive/Cargo.toml"
)

for file in "${required_files[@]}"; do
  test -f "$file"
done
