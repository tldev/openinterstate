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
    --source-metadata-file /abs/path/source-pbf-metadata.json \
    --interstate-relation-cache-file /abs/path/interstate-relations.tsv
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
INTERSTATE_RELATION_CACHE_FILE=""
RAW_PBF=""

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
    --interstate-relation-cache-file)
      INTERSTATE_RELATION_CACHE_FILE="$2"
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
[[ -n "$INTERSTATE_RELATION_CACHE_FILE" ]] || die "--interstate-relation-cache-file is required"

require_cmd osmium
require_cmd python3

mkdir -p \
  "$(dirname "$OUTPUT_PBF")" \
  "$(dirname "$SOURCE_METADATA_FILE")" \
  "$(dirname "$INTERSTATE_RELATION_CACHE_FILE")"

cleanup() {
  if [[ -n "$RAW_PBF" && -f "$RAW_PBF" ]]; then
    rm -f "$RAW_PBF"
  fi
}
trap cleanup EXIT

# shellcheck disable=SC1091
source "$REPO_ROOT/bin/lib.sh"

log "Free space before streamed prefilter"
df -h "$(dirname "$OUTPUT_PBF")" >&2

FILTER_ARGS=()
while IFS= read -r line; do
  FILTER_ARGS+=("$line")
done < <(oi_canonical_filter_args)
RAW_PBF="$(mktemp "${TMPDIR:-/tmp}/openinterstate-source-XXXXXX.osm.pbf")"

log "Downloading raw source PBF to ephemeral runner storage"
python3 "$REPO_ROOT/tooling/ci/stream_source_pbf.py" \
  --url "$SOURCE_URL" \
  --metadata-file "$SOURCE_METADATA_FILE" \
  --output-file "$RAW_PBF"

log "Extracting Interstate relation cache from raw source PBF"
python3 "$REPO_ROOT/tooling/extract_interstate_relations.py" \
  --source-pbf "$RAW_PBF" \
  --output "$INTERSTATE_RELATION_CACHE_FILE"

log "Filtering canonical import PBF"
osmium tags-filter \
  "$RAW_PBF" \
  "${FILTER_ARGS[@]}" \
  --overwrite \
  -o "$OUTPUT_PBF"

[[ -s "$OUTPUT_PBF" ]] || die "filtered PBF is empty: $OUTPUT_PBF"
[[ -s "$INTERSTATE_RELATION_CACHE_FILE" ]] || die "interstate relation cache is empty: $INTERSTATE_RELATION_CACHE_FILE"
osmium fileinfo "$OUTPUT_PBF" >/dev/null

log "Prefilter complete"
du -sh "$OUTPUT_PBF" "$SOURCE_METADATA_FILE" "$INTERSTATE_RELATION_CACHE_FILE" >&2
