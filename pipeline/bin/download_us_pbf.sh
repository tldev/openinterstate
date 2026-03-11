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
  pipeline/bin/download_us_pbf.sh [--url https://...] [--output /abs/path/us-latest.osm.pbf]

Defaults:
  url    -> OI_DEFAULT_US_PBF_URL or https://download.geofabrik.de/north-america/us-latest.osm.pbf
  output -> OI_DOWNLOAD_DIR/<basename(url)> or .data/downloads/<basename(url)>
USAGE
}

SOURCE_URL="${OI_DEFAULT_US_PBF_URL:-https://download.geofabrik.de/north-america/us-latest.osm.pbf}"
OUTPUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      SOURCE_URL="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
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

oi_require_cmd curl
default_dir="${OI_DOWNLOAD_DIR:-$REPO_ROOT/.data/downloads}"
mkdir -p "$default_dir"

if [[ -z "$OUTPUT" ]]; then
  OUTPUT="$default_dir/$(basename "$SOURCE_URL")"
fi

oi_log "Downloading source PBF"
echo "  url: $SOURCE_URL"
echo "  output: $OUTPUT"

curl -L --fail --progress-bar "$SOURCE_URL" -o "$OUTPUT"

oi_log "Download complete"
echo "$OUTPUT"
