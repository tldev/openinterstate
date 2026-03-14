#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  tooling/ci/build_release_host.sh \
    --release-id release-YYYY-MM-DD \
    --filtered-pbf-file /abs/path/us-latest.canonical-filtered.osm.pbf \
    --source-pbf-metadata-file /abs/path/source-pbf-metadata.json \
    --interstate-relation-cache-file /abs/path/interstate-relations.tsv \
    --output-root /abs/path/release-output \
    --work-dir /abs/path/workdir \
    [--source-url URL]
EOF
}

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >&2
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || die "missing required command: $cmd"
}

free_space_gb() {
  local path="$1"
  df -Pk "$path" | awk 'NR == 2 { print int($4 / 1024 / 1024) }'
}

require_free_space_gb() {
  local path="$1"
  local min_gb="$2"
  local available_gb
  available_gb="$(free_space_gb "$path")"
  if (( available_gb < min_gb )); then
    die "need at least ${min_gb}GB free at $path, found ${available_gb}GB"
  fi
}

wait_for_postgres() {
  local attempts="${1:-90}"
  local sleep_seconds="${2:-2}"
  local attempt

  for (( attempt=1; attempt<=attempts; attempt+=1 )); do
    if PGPASSWORD="$DB_PASSWORD" pg_isready \
      -h "$DB_HOST" \
      -p "$DB_PORT" \
      -U "$DB_USER" \
      -d "$DB_NAME" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_seconds"
  done

  return 1
}

RELEASE_ID=""
FILTERED_PBF_FILE=""
SOURCE_PBF_METADATA_FILE=""
INTERSTATE_RELATION_CACHE_FILE=""
SOURCE_URL=""
OUTPUT_ROOT=""
WORK_DIR=""
STATE_DIR=""

DB_HOST="${OI_CI_DB_HOST:-127.0.0.1}"
DB_PORT="${OI_CI_DB_PORT:-55432}"
DB_NAME="${OI_CI_DB_NAME:-osm}"
DB_USER="${OI_CI_DB_USER:-osm}"
DB_PASSWORD="${OI_CI_DB_PASSWORD:-osm_dev}"
MIN_FREE_GB="${OI_CI_MIN_FREE_GB:-7}"
IMPORT_CACHE_MB="${OI_IMPORT_CACHE_MB:-2048}"
DB_CONTAINER_NAME=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release-id)
      RELEASE_ID="$2"
      shift 2
      ;;
    --filtered-pbf-file)
      FILTERED_PBF_FILE="$2"
      shift 2
      ;;
    --source-pbf-metadata-file)
      SOURCE_PBF_METADATA_FILE="$2"
      shift 2
      ;;
    --interstate-relation-cache-file)
      INTERSTATE_RELATION_CACHE_FILE="$2"
      shift 2
      ;;
    --source-url)
      SOURCE_URL="$2"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --work-dir)
      WORK_DIR="$2"
      shift 2
      ;;
    --state-dir)
      STATE_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      die "unknown argument: $1"
      ;;
  esac
done

[[ -n "$RELEASE_ID" ]] || die "--release-id is required"
[[ -n "$FILTERED_PBF_FILE" ]] || die "--filtered-pbf-file is required"
[[ -n "$SOURCE_PBF_METADATA_FILE" ]] || die "--source-pbf-metadata-file is required"
[[ -n "$INTERSTATE_RELATION_CACHE_FILE" ]] || die "--interstate-relation-cache-file is required"
[[ -n "$OUTPUT_ROOT" ]] || die "--output-root is required"
[[ -n "$WORK_DIR" ]] || die "--work-dir is required"

[[ -f "$FILTERED_PBF_FILE" ]] || die "filtered PBF not found: $FILTERED_PBF_FILE"
[[ -f "$SOURCE_PBF_METADATA_FILE" ]] || die "source metadata not found: $SOURCE_PBF_METADATA_FILE"
[[ -f "$INTERSTATE_RELATION_CACHE_FILE" ]] || die "interstate relation cache not found: $INTERSTATE_RELATION_CACHE_FILE"

STATE_DIR="${STATE_DIR:-$WORK_DIR/state}"
DATABASE_URL="postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
POSTGRES_DATA_DIR="$WORK_DIR/postgres"
RELEASE_DIR="$OUTPUT_ROOT/$RELEASE_ID"
ARCHIVE_PATH="$OUTPUT_ROOT/openinterstate-$RELEASE_ID.tar.gz"

require_cmd cargo
require_cmd docker
require_cmd osm2pgsql
require_cmd pg_isready
require_cmd psql
require_cmd python3
require_cmd tar

mkdir -p "$OUTPUT_ROOT" "$POSTGRES_DATA_DIR" "$STATE_DIR"
rm -rf "$RELEASE_DIR" "$ARCHIVE_PATH"

cleanup() {
  local exit_code=$?
  if [[ -n "$DB_CONTAINER_NAME" ]]; then
    if (( exit_code != 0 )); then
      docker logs "$DB_CONTAINER_NAME" >&2 || true
    fi
    docker rm -f "$DB_CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

log "Free space before release build"
df -h "$WORK_DIR" >&2
require_free_space_gb "$WORK_DIR" "$MIN_FREE_GB"

DB_CONTAINER_NAME="openinterstate-ci-db-${RANDOM}-$$"
log "Starting PostGIS container $DB_CONTAINER_NAME"
docker run \
  --detach \
  --rm \
  --name "$DB_CONTAINER_NAME" \
  --shm-size 2g \
  -e POSTGRES_DB="$DB_NAME" \
  -e POSTGRES_USER="$DB_USER" \
  -e POSTGRES_PASSWORD="$DB_PASSWORD" \
  -p "${DB_PORT}:5432" \
  -v "$POSTGRES_DATA_DIR:/var/lib/postgresql/data" \
  postgis/postgis:16-3.4 \
  postgres \
    -c shared_buffers=512MB \
    -c effective_cache_size=2GB \
    -c maintenance_work_mem=512MB \
    -c work_mem=32MB \
    -c max_wal_size=32GB \
    -c min_wal_size=8GB \
    -c checkpoint_timeout=60min \
    -c checkpoint_completion_target=0.9 \
    -c wal_compression=on \
    -c wal_level=minimal \
    -c max_wal_senders=0 \
    -c archive_mode=off \
    -c synchronous_commit=off \
    -c fsync=off \
    -c full_page_writes=off \
    -c autovacuum=off \
    -c effective_io_concurrency=200 \
    -c random_page_cost=1.1 \
    >/dev/null

wait_for_postgres || die "PostGIS container did not become ready"

log "Bootstrapping database schema"
PGPASSWORD="$DB_PASSWORD" psql \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 \
  -c "CREATE EXTENSION IF NOT EXISTS postgis;"
PGPASSWORD="$DB_PASSWORD" psql \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 \
  -f "$REPO_ROOT/schema/bootstrap.sql"

log "Importing canonical filtered PBF"
PGPASSWORD="$DB_PASSWORD" osm2pgsql \
  --slim \
  --create \
  --output=flex \
  --style="$REPO_ROOT/schema/osm2pgsql/openinterstate.lua" \
  --database="$DB_NAME" \
  --host="$DB_HOST" \
  --port="$DB_PORT" \
  -U "$DB_USER" \
  --cache="$IMPORT_CACHE_MB" \
  "$FILTERED_PBF_FILE"

log "Applying deterministic SQL projection"
PGPASSWORD="$DB_PASSWORD" psql \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 \
  -f "$REPO_ROOT/schema/derive.sql"

log "Building graph, corridors, and reference routes"
DERIVE_ARGS=(
  cargo
  run
  --locked
  --release
  -p
  openinterstate-derive
  --
  --database-url
  "$DATABASE_URL"
  --interstate-relation-cache
  "$INTERSTATE_RELATION_CACHE_FILE"
)
DERIVE_ARGS+=(all)
"${DERIVE_ARGS[@]}"

log "Exporting release artifacts"
EXPORT_ARGS=(
  python3
  "$REPO_ROOT/tooling/export_release.py"
  --database-url "$DATABASE_URL"
  --release-id "$RELEASE_ID"
  --output-dir "$RELEASE_DIR"
  --state-dir "$STATE_DIR"
  --source-pbf-metadata-file "$SOURCE_PBF_METADATA_FILE"
  --import-pbf-file "$FILTERED_PBF_FILE"
)
if [[ -n "$SOURCE_URL" ]]; then
  EXPORT_ARGS+=(--source-url "$SOURCE_URL")
fi
"${EXPORT_ARGS[@]}"

find "$RELEASE_DIR" \
  \( -name '.DS_Store' -o -name '._*' \) \
  -type f \
  -delete

log "Packaging release archive"
tar \
  --exclude='.DS_Store' \
  --exclude='._*' \
  -C "$OUTPUT_ROOT" \
  -czf "$ARCHIVE_PATH" \
  "$RELEASE_ID"

log "Release build complete"
du -sh "$FILTERED_PBF_FILE" "$RELEASE_DIR" "$ARCHIVE_PATH" >&2
if ! du -sh "$POSTGRES_DATA_DIR" >&2 2>/dev/null; then
  log "Skipping postgres size summary; directory is owned by the container user"
fi
