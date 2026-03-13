#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  tooling/ci/prefilter_stream.sh \
    --source-url URL \
    --output-pbf /abs/path/us-latest.canonical-filtered.osm.pbf \
    --source-metadata-file /abs/path/source-pbf-metadata.json
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

SOURCE_URL=""
OUTPUT_PBF=""
SOURCE_METADATA_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-url)
      SOURCE_URL="$2"
      shift 2
      ;;
    --output-pbf)
      OUTPUT_PBF="$2"
      shift 2
      ;;
    --source-metadata-file)
      SOURCE_METADATA_FILE="$2"
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

[[ -n "$SOURCE_URL" ]] || die "--source-url is required"
[[ -n "$OUTPUT_PBF" ]] || die "--output-pbf is required"
[[ -n "$SOURCE_METADATA_FILE" ]] || die "--source-metadata-file is required"

require_cmd osmium
require_cmd python3

mkdir -p "$(dirname "$OUTPUT_PBF")" "$(dirname "$SOURCE_METADATA_FILE")"

# shellcheck disable=SC1091
source "$REPO_ROOT/bin/lib.sh"

log "Free space before streamed prefilter"
df -h "$(dirname "$OUTPUT_PBF")" >&2

mapfile -t FILTER_ARGS < <(oi_canonical_filter_args)

log "Streaming raw source PBF into canonical filter"
python3 "$REPO_ROOT/tooling/ci/stream_source_pbf.py" \
  --url "$SOURCE_URL" \
  --metadata-file "$SOURCE_METADATA_FILE" \
  | osmium tags-filter \
      -F pbf \
      - \
      "${FILTER_ARGS[@]}" \
      --overwrite \
      -o "$OUTPUT_PBF"

[[ -s "$OUTPUT_PBF" ]] || die "filtered PBF is empty: $OUTPUT_PBF"
osmium fileinfo "$OUTPUT_PBF" >/dev/null

log "Prefilter complete"
du -sh "$OUTPUT_PBF" "$SOURCE_METADATA_FILE" >&2
