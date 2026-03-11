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
  pipeline/bin/publish_release.sh --release-id release-YYYY-MM-DD [--output-root /abs/path/build]
USAGE
}

RELEASE_ID=""
OUTPUT_ROOT="${OI_BUILD_DIR:-$REPO_ROOT/build}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release-id)
      RELEASE_ID="$2"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="$2"
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

if [[ -z "$RELEASE_ID" ]]; then
  usage
  exit 1
fi

oi_require_cmd gh

RELEASE_DIR="$OUTPUT_ROOT/$RELEASE_ID"
ARCHIVE_PATH="$OUTPUT_ROOT/openinterstate-$RELEASE_ID.tar.gz"
MANIFEST_PATH="$RELEASE_DIR/manifest.json"
CHECKSUMS_PATH="$RELEASE_DIR/checksums.txt"
SOURCE_LINEAGE_PATH="$RELEASE_DIR/source_lineage.json"

for path in "$RELEASE_DIR" "$ARCHIVE_PATH" "$MANIFEST_PATH" "$CHECKSUMS_PATH" "$SOURCE_LINEAGE_PATH"; do
  if [[ ! -e "$path" ]]; then
    oi_die "missing publish artifact: $path"
  fi
done

RELEASE_NOTES="Rebuilt from the raw U.S. OSM PBF using the standalone OpenInterstate pipeline. See manifest.json and source_lineage.json for raw-source and imported-filter lineage, including SHA-256 hashes."

if gh release view "$RELEASE_ID" --repo tldev/openinterstate >/dev/null 2>&1; then
  gh release upload "$RELEASE_ID" \
    "$ARCHIVE_PATH" \
    "$MANIFEST_PATH" \
    "$CHECKSUMS_PATH" \
    "$SOURCE_LINEAGE_PATH" \
    --repo tldev/openinterstate \
    --clobber
  gh release edit "$RELEASE_ID" \
    --repo tldev/openinterstate \
    --title "$RELEASE_ID" \
    --notes "$RELEASE_NOTES"
else
  gh release create "$RELEASE_ID" \
    "$ARCHIVE_PATH" \
    "$MANIFEST_PATH" \
    "$CHECKSUMS_PATH" \
    "$SOURCE_LINEAGE_PATH" \
    --repo tldev/openinterstate \
    --title "$RELEASE_ID" \
    --notes "$RELEASE_NOTES"
fi
