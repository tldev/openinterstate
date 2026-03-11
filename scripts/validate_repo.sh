#!/usr/bin/env bash
set -euo pipefail

required_files=(
  "README.md"
  "CONTRIBUTING.md"
  "CODEOWNERS"
  "DATA_LICENSE.md"
  "docs/charter.md"
  "docs/naming_glossary.md"
  "docs/data_contract_draft.md"
  "docs/licensing_and_attribution.md"
  "docs/release_format.md"
  "docs/roadmap.md"
  "docs/program_status.md"
  "docs/external_blockers.md"
  "api/openapi.yaml"
  "schemas/v0/manifest.schema.json"
  "examples/duckdb/example_queries.sql"
)

for file in "${required_files[@]}"; do
  test -f "$file"
done
