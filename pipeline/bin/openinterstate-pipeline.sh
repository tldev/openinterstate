#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

ENV_FILE="${ENV_FILE:-$DEFAULT_ENV_FILE}"

usage() {
  cat <<'USAGE'
Usage:
  pipeline/bin/openinterstate-pipeline.sh [--env-file /abs/path/.env.pipeline] <command> [args]

Commands:
  run               Import canonical OSM, derive product tables, and export a public release
  download-us       Download the latest U.S. Geofabrik PBF
  import-canonical  Run canonical osm2pgsql import
  derive            Apply deterministic product derivation and graph builds
  release           Export public release artifacts from populated product tables
  publish           Create a GitHub release from a built artifact set
USAGE
}

if [[ "${1:-}" == "--env-file" ]]; then
  if [[ $# -lt 3 ]]; then
    usage
    exit 1
  fi
  ENV_FILE="$2"
  shift 2
fi

COMMAND="${1:-}"
if [[ -z "$COMMAND" ]]; then
  usage
  exit 1
fi
shift || true

case "$COMMAND" in
  run)
    ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/pipeline.sh" "$@"
    ;;
  download-us)
    ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/download_us_pbf.sh" "$@"
    ;;
  import-canonical)
    ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/import_canonical_osm.sh" "$@"
    ;;
  derive)
    ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/derive_product.sh" "$@"
    ;;
  release)
    ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/export_release.sh" "$@"
    ;;
  publish)
    ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/publish_release.sh" "$@"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: $COMMAND" >&2
    usage
    exit 1
    ;;
esac
