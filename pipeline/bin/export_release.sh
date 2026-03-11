#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

ENV_FILE="${ENV_FILE:-$DEFAULT_ENV_FILE}"
oi_load_env "$ENV_FILE"

usage() {
  cat <<'USAGE'
Usage:
  pipeline/bin/export_release.sh \
    --release-id release-YYYY-MM-DD \
    --source-pbf-file /abs/path/us-latest.osm.pbf \
    [--import-pbf-file /abs/path/us-latest.canonical-filtered.osm.pbf] \
    [--source-url https://download.geofabrik.de/north-america/us-latest.osm.pbf] \
    [--output-root /abs/path/build]
USAGE
}

RELEASE_ID=""
SOURCE_PBF_FILE=""
IMPORT_PBF_FILE=""
SOURCE_URL=""
OUTPUT_ROOT="${OI_BUILD_DIR:-$REPO_ROOT/build}"
DATABASE_URL=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release-id)
      RELEASE_ID="$2"
      shift 2
      ;;
    --source-pbf-file)
      SOURCE_PBF_FILE="$2"
      shift 2
      ;;
    --import-pbf-file)
      IMPORT_PBF_FILE="$2"
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
    --database-url)
      DATABASE_URL="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$RELEASE_ID" || -z "$SOURCE_PBF_FILE" ]]; then
  usage
  exit 1
fi

if [[ -z "$DATABASE_URL" ]]; then
  if [[ -n "${OSM_DB_HOST:-}" && -n "${OSM_DB_PORT:-}" && -n "${OSM_DB_NAME:-}" && -n "${OSM_DB_USER:-}" && -n "${OSM_DB_PASSWORD:-}" ]]; then
    DATABASE_URL="postgres://${OSM_DB_USER}:${OSM_DB_PASSWORD}@${OSM_DB_HOST}:${OSM_DB_PORT}/${OSM_DB_NAME}"
  else
    DATABASE_URL="${PRODUCT_DB_URL:-postgres://osm:osm_dev@osm-db:5432/osm}"
  fi
fi

mkdir -p "$OUTPUT_ROOT"
OUTPUT_DIR="$OUTPUT_ROOT/$RELEASE_ID"
ARCHIVE_PATH="$OUTPUT_ROOT/openinterstate-$RELEASE_ID.tar.gz"

oi_ensure_python_env

oi_log "Exporting OpenInterstate release"
echo "  release id: $RELEASE_ID"
echo "  source pbf: $SOURCE_PBF_FILE"
echo "  import pbf: ${IMPORT_PBF_FILE:-$SOURCE_PBF_FILE}"
echo "  database url: $DATABASE_URL"
echo "  output dir: $OUTPUT_DIR"

"$OI_VENV_PYTHON" "$REPO_ROOT/scripts/export_v1_release.py" \
  --database-url "$DATABASE_URL" \
  --release-id "$RELEASE_ID" \
  --output-dir "$OUTPUT_DIR" \
  --source-pbf-file "$SOURCE_PBF_FILE" \
  --import-pbf-file "${IMPORT_PBF_FILE:-$SOURCE_PBF_FILE}" \
  ${SOURCE_URL:+--source-url "$SOURCE_URL"}

tar -C "$OUTPUT_ROOT" -czf "$ARCHIVE_PATH" "$RELEASE_ID"

oi_log "Release export complete"
echo "  release dir: $OUTPUT_DIR"
echo "  archive: $ARCHIVE_PATH"
