#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  pipeline/bin/filter_canonical_pbf.sh --input /abs/path/conus.osm.pbf --output /abs/path/conus.canonical-filtered.osm.pbf [--force]

This deterministic filter keeps only OSM features needed by the canonical mapping:
  - exit nodes (highway=motorway_junction)
  - POI node/way categories used by the OpenInterstate flex mapping
  - highway ways needed for corridor assignment:
      motorway, motorway_link, trunk, trunk_link, primary, primary_link

Idempotency behavior:
  - If output exists, is non-empty, and input is not newer: skip filter work.
  - Use --force to rebuild output regardless of timestamps.
USAGE
}

INPUT_PBF=""
OUTPUT_PBF=""
FORCE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)
      INPUT_PBF="$2"
      shift 2
      ;;
    --output)
      OUTPUT_PBF="$2"
      shift 2
      ;;
    --force)
      FORCE=true
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

if [[ -z "$INPUT_PBF" || -z "$OUTPUT_PBF" ]]; then
  usage
  exit 1
fi

if [[ ! -f "$INPUT_PBF" ]]; then
  echo "Input PBF not found: $INPUT_PBF" >&2
  exit 1
fi

OSMIUM_BIN="${OSMIUM_BIN:-osmium}"
if ! command -v "$OSMIUM_BIN" >/dev/null 2>&1; then
  echo "Missing required binary: osmium (install with: brew install osmium-tool)" >&2
  exit 1
fi

OUTPUT_DIR="$(cd "$(dirname "$OUTPUT_PBF")" && pwd)"
mkdir -p "$OUTPUT_DIR"
OUTPUT_BASENAME="$(basename "$OUTPUT_PBF")"
OUTPUT_PATH="$OUTPUT_DIR/$OUTPUT_BASENAME"

if [[ "$FORCE" != true && -s "$OUTPUT_PATH" && ! "$INPUT_PBF" -nt "$OUTPUT_PATH" ]]; then
  echo "Skipping canonical filter (output is current):"
  echo "  input:  $INPUT_PBF"
  echo "  output: $OUTPUT_PATH"
  exit 0
fi

if [[ "$OUTPUT_PATH" == *.osm.pbf ]]; then
  OUTPUT_TMP="${OUTPUT_PATH%.osm.pbf}.tmp.$$.osm.pbf"
else
  OUTPUT_TMP="${OUTPUT_PATH}.tmp.$$"
fi
rm -f "$OUTPUT_TMP"

echo "Filtering canonical PBF:"
echo "  input:  $INPUT_PBF"
echo "  output: $OUTPUT_PATH"

# Notes:
# - We intentionally do NOT use --omit-referenced, so ways keep referenced nodes.
# - Key-only expressions (n/cuisine, w/cuisine) match any value.
"$OSMIUM_BIN" tags-filter \
  "$INPUT_PBF" \
  n/highway=motorway_junction \
  n/amenity=fuel,restaurant,fast_food,cafe,toilets,charging_station \
  n/tourism=hotel,motel,guest_house \
  n/shop=gas \
  n/cuisine \
  n/highway=rest_area,services \
  w/highway=motorway,motorway_link,trunk,trunk_link,primary,primary_link,rest_area,services \
  w/amenity=fuel,restaurant,fast_food,cafe,toilets,charging_station \
  w/tourism=hotel,motel,guest_house \
  w/shop=gas \
  w/cuisine \
  --overwrite \
  -o "$OUTPUT_TMP"

mv "$OUTPUT_TMP" "$OUTPUT_PATH"

echo "Filter complete"
