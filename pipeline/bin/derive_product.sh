#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

ENV_FILE="${ENV_FILE:-$DEFAULT_ENV_FILE}"
SQL_FILE="$PIPELINE_DIR/sql/derive_product.sql"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "Missing SQL file: $SQL_FILE" >&2
  exit 1
fi

oi_load_env "$ENV_FILE"
oi_guard_no_reachability_clears "$SQL_FILE"
oi_compose_cmd "$ENV_FILE"

PRODUCT_DB_URL="${PRODUCT_DB_URL:-postgres://osm:osm_dev@osm-db:5432/osm}"
OSM_DB_HOST="${OSM_DB_HOST:-localhost}"
OSM_DB_PORT="${OSM_DB_PORT:-5434}"
OSM_DB_NAME="${OSM_DB_NAME:-osm}"
OSM_DB_USER="${OSM_DB_USER:-osm}"
OSM_DB_PASSWORD="${OSM_DB_PASSWORD:-osm_dev}"

echo "Applying product derivation SQL"
echo "  db: $PRODUCT_DB_URL"
echo "  sql: $SQL_FILE"

if command -v psql >/dev/null 2>&1; then
  PGPASSWORD="$OSM_DB_PASSWORD" psql \
    -h "$OSM_DB_HOST" \
    -p "$OSM_DB_PORT" \
    -U "$OSM_DB_USER" \
    -d "$OSM_DB_NAME" \
    -v ON_ERROR_STOP=1 \
    -f "$SQL_FILE"
else
  "${OI_COMPOSE_CMD[@]}" exec -T osm-db \
    psql -U "$OSM_DB_USER" -d "$OSM_DB_NAME" -v ON_ERROR_STOP=1 < "$SQL_FILE"
fi

echo "Building highway graph from osm2pgsql tables"
"${OI_COMPOSE_CMD[@]}" run --rm rust cargo run --release -p openinterstate-import -- \
  --database-url "$PRODUCT_DB_URL" \
  --build-graph-only

echo "Building corridors from highway graph"
"${OI_COMPOSE_CMD[@]}" run --rm rust cargo run --release -p openinterstate-import -- \
  --database-url "$PRODUCT_DB_URL" \
  --build-corridors-only

echo "Building reference routes from derived highway graph"
"${OI_COMPOSE_CMD[@]}" run --rm rust cargo run --release -p openinterstate-import -- \
  --database-url "$PRODUCT_DB_URL" \
  --build-reference-routes-only

echo "Product derivation complete"
