#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tooling/ci/publish_release.sh \
    --release-id release-YYYY-MM-DD \
    --release-dir /abs/path/release-YYYY-MM-DD \
    --archive-file /abs/path/openinterstate-release-YYYY-MM-DD.tar.gz \
    [--repo owner/name]
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || die "missing required command: $cmd"
}

RELEASE_ID=""
RELEASE_DIR=""
ARCHIVE_FILE=""
REPO="tldev/openinterstate"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release-id)
      RELEASE_ID="$2"
      shift 2
      ;;
    --release-dir)
      RELEASE_DIR="$2"
      shift 2
      ;;
    --archive-file)
      ARCHIVE_FILE="$2"
      shift 2
      ;;
    --repo)
      REPO="$2"
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
[[ -n "$RELEASE_DIR" ]] || die "--release-dir is required"
[[ -n "$ARCHIVE_FILE" ]] || die "--archive-file is required"

require_cmd gh

MANIFEST_PATH="$RELEASE_DIR/manifest.json"
CHECKSUMS_PATH="$RELEASE_DIR/checksums.txt"
SOURCE_LINEAGE_PATH="$RELEASE_DIR/source_lineage.json"

for path in "$RELEASE_DIR" "$ARCHIVE_FILE" "$MANIFEST_PATH" "$CHECKSUMS_PATH" "$SOURCE_LINEAGE_PATH"; do
  [[ -e "$path" ]] || die "missing publish artifact: $path"
done

RELEASE_NOTES="Rebuilt from the raw U.S. OSM PBF using the standalone OpenInterstate pipeline. See manifest.json and source_lineage.json for raw-source and imported-filter lineage, including SHA-256 hashes."

if gh release view "$RELEASE_ID" --repo "$REPO" >/dev/null 2>&1; then
  gh release upload "$RELEASE_ID" \
    "$ARCHIVE_FILE" \
    "$MANIFEST_PATH" \
    "$CHECKSUMS_PATH" \
    "$SOURCE_LINEAGE_PATH" \
    --repo "$REPO" \
    --clobber
  gh release edit "$RELEASE_ID" \
    --repo "$REPO" \
    --title "$RELEASE_ID" \
    --notes "$RELEASE_NOTES"
else
  gh release create "$RELEASE_ID" \
    "$ARCHIVE_FILE" \
    "$MANIFEST_PATH" \
    "$CHECKSUMS_PATH" \
    "$SOURCE_LINEAGE_PATH" \
    --repo "$REPO" \
    --title "$RELEASE_ID" \
    --notes "$RELEASE_NOTES"
fi
