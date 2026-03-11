#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

ENV_FILE="${ENV_FILE:-$DEFAULT_ENV_FILE}"
oi_load_env "$ENV_FILE"
oi_compose_cmd "$ENV_FILE"

usage() {
  cat <<'USAGE'
Usage:
  pipeline/bin/pipeline.sh (--pbf-file /abs/path/us-latest.osm.pbf | --pbf-url https://...) [options]

Options:
  --release-id release-YYYY-MM-DD
  --output-root /abs/path/build
  --source-url https://...
  --no-prefilter
  --force-prefilter

Notes:
  - If --pbf-url is used, the source file is downloaded first.
  - This command starts the local PostGIS service automatically.
USAGE
}

PBF_FILE=""
PBF_URL=""
SOURCE_URL=""
RELEASE_ID="release-$(date +%F)"
OUTPUT_ROOT="${OI_BUILD_DIR:-$REPO_ROOT/build}"
PREFILTER=true
FORCE_PREFILTER=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pbf-file)
      PBF_FILE="$2"
      shift 2
      ;;
    --pbf-url)
      PBF_URL="$2"
      shift 2
      ;;
    --source-url)
      SOURCE_URL="$2"
      shift 2
      ;;
    --release-id)
      RELEASE_ID="$2"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --no-prefilter)
      PREFILTER=false
      shift 1
      ;;
    --force-prefilter)
      FORCE_PREFILTER=true
      shift 1
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

if [[ -n "$PBF_FILE" && -n "$PBF_URL" ]]; then
  oi_die "choose either --pbf-file or --pbf-url"
fi

if [[ -z "$PBF_FILE" && -z "$PBF_URL" ]]; then
  usage
  exit 1
fi

if [[ -n "$PBF_URL" ]]; then
  PBF_FILE="$(ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/download_us_pbf.sh" --url "$PBF_URL")"
  if [[ -z "$SOURCE_URL" ]]; then
    SOURCE_URL="$PBF_URL"
  fi
fi

mkdir -p "$OUTPUT_ROOT"

oi_log "Starting local PostGIS service"
"${OI_COMPOSE_CMD[@]}" up -d osm-db

resolved_import_file="$(mktemp)"
trap 'rm -f "$resolved_import_file"' EXIT

import_args=(--pbf-file "$PBF_FILE" --resolved-import-pbf-out "$resolved_import_file")
if [[ "$PREFILTER" != true ]]; then
  import_args+=(--no-prefilter)
fi
if [[ "$FORCE_PREFILTER" == true ]]; then
  import_args+=(--force-prefilter)
fi

until "${OI_COMPOSE_CMD[@]}" exec -T osm-db pg_isready -U "${OSM_DB_USER:-osm}" -d "${OSM_DB_NAME:-osm}" >/dev/null 2>&1; do
  sleep 2
done

echo "[1/3] canonical import"
ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/import_canonical_osm.sh" "${import_args[@]}"

echo "[2/3] deterministic derive"
ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/derive_product.sh"

echo "[3/3] public release export"
ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/export_release.sh" \
  --release-id "$RELEASE_ID" \
  --source-pbf-file "$PBF_FILE" \
  --import-pbf-file "$(cat "$resolved_import_file")" \
  ${SOURCE_URL:+--source-url "$SOURCE_URL"} \
  --output-root "$OUTPUT_ROOT"

oi_log "Pipeline complete"
