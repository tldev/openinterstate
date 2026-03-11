#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

ENV_FILE="${ENV_FILE:-$DEFAULT_ENV_FILE}"
oi_load_env "$ENV_FILE"
oi_compose_cmd "$ENV_FILE"

MAPPING_FILE="$REPO_ROOT/config/osm2pgsql/openinterstate_v1.lua"
FILTER_SCRIPT="$PIPELINE_DIR/bin/filter_canonical_pbf.sh"

if [[ ! -f "$MAPPING_FILE" ]]; then
  oi_die "missing mapping file: $MAPPING_FILE"
fi

usage() {
  cat <<'USAGE'
Usage:
  pipeline/bin/import_canonical_osm.sh --pbf-file /abs/path/us-latest.osm.pbf [--no-prefilter] [--prefilter-output /abs/path/filtered.osm.pbf] [--force-prefilter]
  pipeline/bin/import_canonical_osm.sh --pbf-dir /abs/path/extracts --merge-output /abs/path/staging/us.osm.pbf [--no-prefilter] [--prefilter-output /abs/path/filtered.osm.pbf] [--force-merge] [--force-prefilter]

Notes:
  - Canonical prefilter is enabled by default.
  - Use --no-prefilter to import raw PBF directly.
  - Existing canonical tables are appended by default; destructive reset requires
    OSM2PGSQL_MODE=create and OI_ALLOW_CANONICAL_RESET=true.
USAGE
}

is_output_current_for_inputs() {
  local output="$1"
  shift

  if [[ ! -s "$output" ]]; then
    return 1
  fi

  local input
  for input in "$@"; do
    if [[ "$input" -nt "$output" ]]; then
      return 1
    fi
  done

  return 0
}

canonical_tables_exist() {
  if command -v psql >/dev/null 2>&1; then
    local exists
    exists="$(
      PGPASSWORD="$OSM_DB_PASSWORD" psql \
        -h "$OSM_DB_HOST" \
        -p "$OSM_DB_PORT" \
        -U "$OSM_DB_USER" \
        -d "$OSM_DB_NAME" \
        -Atc "SELECT CASE WHEN to_regclass('public.osm2pgsql_v2_highways') IS NULL THEN 0 ELSE 1 END;" 2>/dev/null || true
    )"

    case "$exists" in
      1) return 0 ;;
      0) return 1 ;;
      *) return 2 ;;
    esac
  fi

  local compose_out
  compose_out="$("${OI_COMPOSE_CMD[@]}" exec -T osm-db \
    psql -U "$OSM_DB_USER" -d "$OSM_DB_NAME" \
    -Atc "SELECT CASE WHEN to_regclass('public.osm2pgsql_v2_highways') IS NULL THEN 0 ELSE 1 END;" 2>/dev/null || true)"

  case "$compose_out" in
    1) return 0 ;;
    0) return 1 ;;
    *) return 2 ;;
  esac
}

canonical_db_updatable() {
  local raw=""

  if command -v psql >/dev/null 2>&1; then
    raw="$(
      PGPASSWORD="$OSM_DB_PASSWORD" psql \
        -h "$OSM_DB_HOST" \
        -p "$OSM_DB_PORT" \
        -U "$OSM_DB_USER" \
        -d "$OSM_DB_NAME" \
        -Atc "SELECT value FROM osm2pgsql_properties WHERE property = 'updatable' LIMIT 1;" 2>/dev/null || true
    )"
  else
    raw="$("${OI_COMPOSE_CMD[@]}" exec -T osm-db \
      psql -U "$OSM_DB_USER" -d "$OSM_DB_NAME" \
      -Atc "SELECT value FROM osm2pgsql_properties WHERE property = 'updatable' LIMIT 1;" 2>/dev/null || true)"
  fi

  case "$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')" in
    true|1|yes|y|on) return 0 ;;
    false|0|no|n|off) return 1 ;;
    *) return 2 ;;
  esac
}

resolve_import_mode() {
  local requested_mode="${OSM2PGSQL_MODE:-auto}"
  local canonical_state="unknown"
  local detected_mode="append"
  local state_rc=0

  case "$requested_mode" in
    auto|append|create)
      ;;
    *)
      oi_die "invalid OSM2PGSQL_MODE=$requested_mode (expected auto|append|create)"
      ;;
  esac

  if canonical_tables_exist; then
    canonical_state="present"
  else
    state_rc=$?
    if [[ $state_rc -eq 1 ]]; then
      canonical_state="missing"
    fi
  fi

  case "$requested_mode" in
    append)
      detected_mode="append"
      ;;
    create)
      detected_mode="create"
      ;;
    auto)
      case "$canonical_state" in
        present) detected_mode="append" ;;
        missing) detected_mode="create" ;;
        unknown)
          detected_mode="append"
          echo "Warning: canonical table probe unavailable; defaulting to append mode" >&2
          ;;
      esac
      ;;
  esac

  if [[ "$detected_mode" == "create" && "$canonical_state" == "present" ]]; then
    if ! oi_is_truthy "${OI_ALLOW_CANONICAL_RESET:-false}"; then
      oi_die "refusing canonical reset: set OI_ALLOW_CANONICAL_RESET=true to permit create mode"
    fi
  fi

  printf '%s' "$detected_mode"
}

PBF_FILE=""
PBF_DIR=""
MERGE_OUTPUT=""
PREFILTER=true
PREFILTER_OUTPUT=""
FORCE_MERGE=false
FORCE_PREFILTER=false
RESOLVED_IMPORT_PBF_OUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pbf-file)
      PBF_FILE="$2"
      shift 2
      ;;
    --pbf-dir)
      PBF_DIR="$2"
      shift 2
      ;;
    --merge-output)
      MERGE_OUTPUT="$2"
      shift 2
      ;;
    --prefilter)
      PREFILTER=true
      shift 1
      ;;
    --no-prefilter)
      PREFILTER=false
      shift 1
      ;;
    --prefilter-output)
      PREFILTER_OUTPUT="$2"
      shift 2
      ;;
    --force-merge)
      FORCE_MERGE=true
      shift 1
      ;;
    --force-prefilter)
      FORCE_PREFILTER=true
      shift 1
      ;;
    --resolved-import-pbf-out)
      RESOLVED_IMPORT_PBF_OUT="$2"
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

if [[ -z "$PBF_FILE" && -z "$PBF_DIR" ]]; then
  usage
  exit 1
fi

OSM_DB_HOST="${OSM_DB_HOST:-localhost}"
OSM_DB_PORT="${OSM_DB_PORT:-5434}"
OSM_DB_NAME="${OSM_DB_NAME:-osm}"
OSM_DB_USER="${OSM_DB_USER:-osm}"
OSM_DB_PASSWORD="${OSM_DB_PASSWORD:-osm_dev}"

OSM2PGSQL_BIN="${OSM2PGSQL_BIN:-osm2pgsql}"
OSMIUM_BIN="${OSMIUM_BIN:-osmium}"
oi_require_cmd "$OSM2PGSQL_BIN"

if [[ -n "$PBF_DIR" ]]; then
  oi_require_cmd "$OSMIUM_BIN"
  PBF_FILES=()
  while IFS= read -r pbf; do
    PBF_FILES+=("$pbf")
  done < <(
    find "$PBF_DIR" -maxdepth 1 -type f -name '*.osm.pbf' \
      ! -name '*.canonical-filtered.osm.pbf' \
      ! -name '*.tmp.*.osm.pbf' \
      | sort
  )

  if [[ ${#PBF_FILES[@]} -eq 0 ]]; then
    oi_die "no .osm.pbf files found in $PBF_DIR"
  fi

  if [[ ${#PBF_FILES[@]} -eq 1 ]]; then
    PBF_FILE="${PBF_FILES[0]}"
  else
    if [[ -z "$MERGE_OUTPUT" ]]; then
      oi_die "--merge-output is required when --pbf-dir contains multiple files"
    fi

    mkdir -p "$(dirname "$MERGE_OUTPUT")"
    if [[ "$FORCE_MERGE" != true ]] && is_output_current_for_inputs "$MERGE_OUTPUT" "${PBF_FILES[@]}"; then
      oi_log "Skipping PBF merge (output is current): $MERGE_OUTPUT"
    else
      oi_log "Merging ${#PBF_FILES[@]} PBF files into $MERGE_OUTPUT"
      if [[ "$MERGE_OUTPUT" == *.osm.pbf ]]; then
        MERGE_OUTPUT_TMP="${MERGE_OUTPUT%.osm.pbf}.tmp.$$.osm.pbf"
      else
        MERGE_OUTPUT_TMP="${MERGE_OUTPUT}.tmp.$$"
      fi
      rm -f "$MERGE_OUTPUT_TMP"
      "$OSMIUM_BIN" merge "${PBF_FILES[@]}" --overwrite -o "$MERGE_OUTPUT_TMP"
      mv "$MERGE_OUTPUT_TMP" "$MERGE_OUTPUT"
    fi
    PBF_FILE="$MERGE_OUTPUT"
  fi
fi

if [[ ! -f "$PBF_FILE" ]]; then
  oi_die "PBF file not found: $PBF_FILE"
fi

IMPORT_PBF_FILE="$PBF_FILE"
if [[ "$PREFILTER" == true ]]; then
  if [[ ! -f "$FILTER_SCRIPT" ]]; then
    oi_die "Missing filter script: $FILTER_SCRIPT"
  fi

  if [[ -z "$PREFILTER_OUTPUT" ]]; then
    pbf_basename="$(basename "$PBF_FILE")"
    pbf_stem="${pbf_basename%.osm.pbf}"
    if [[ "$pbf_stem" == "$pbf_basename" ]]; then
      pbf_stem="${pbf_basename%.*}"
    fi
    filtered_dir="${OI_FILTERED_PBF_DIR:-$(cd "$(dirname "$PBF_FILE")" && pwd)/filtered}"
    mkdir -p "$filtered_dir"
    PREFILTER_OUTPUT="$filtered_dir/${pbf_stem}.canonical-filtered.osm.pbf"
  fi

  FILTER_ARGS=(--input "$PBF_FILE" --output "$PREFILTER_OUTPUT")
  if [[ "$FORCE_PREFILTER" == true ]]; then
    FILTER_ARGS+=(--force)
  fi
  "$FILTER_SCRIPT" "${FILTER_ARGS[@]}"
  IMPORT_PBF_FILE="$PREFILTER_OUTPUT"
fi

IMPORT_MODE="$(resolve_import_mode)"

if [[ "$IMPORT_MODE" == "append" ]]; then
  if canonical_db_updatable; then
    :
  else
    case "$?" in
      1)
        echo "Warning: canonical osm2pgsql DB is not updatable (previous create+drop mode)." >&2
        echo "Skipping canonical import to preserve existing OSM data." >&2
        echo "To re-enable append imports, run a one-off reset with:" >&2
        echo "  OSM2PGSQL_MODE=create OSM2PGSQL_DROP_MIDDLE=false OI_ALLOW_CANONICAL_RESET=true" >&2
        exit 0
        ;;
      *)
        echo "Warning: unable to determine canonical updatable state; proceeding with append attempt." >&2
        ;;
    esac
  fi
fi

run_import() {
  local label="$1"
  local host="$2"
  local port="$3"
  local db="$4"
  local user="$5"
  local pass="$6"
  local input_pbf="$7"
  local mode="$8"

  local pbf_basename
  local pbf_stem
  local flatnodes_dir
  local flatnodes_dir_abs
  local flatnodes_path
  local cache_mb
  local drop_middle
  local osm2pgsql_args=()

  pbf_basename="$(basename "$input_pbf")"
  pbf_stem="${pbf_basename%.osm.pbf}"
  if [[ "$pbf_stem" == "$pbf_basename" ]]; then
    pbf_stem="${pbf_basename%.*}"
  fi

  flatnodes_dir="${OSM2PGSQL_FLAT_NODES_DIR:-${OI_FLATNODES_DIR:-$REPO_ROOT/.data/flatnodes}}"
  mkdir -p "$flatnodes_dir"
  flatnodes_dir_abs="$(cd "$flatnodes_dir" && pwd)"
  flatnodes_path="$flatnodes_dir_abs/${label}_${db}_${pbf_stem}.flatnodes.bin"

  cache_mb="${OSM2PGSQL_CACHE_MB:-0}"
  drop_middle="${OSM2PGSQL_DROP_MIDDLE:-false}"

  osm2pgsql_args=(
    --slim
    --cache="$cache_mb"
    --flat-nodes="$flatnodes_path"
    --output=flex
    --style="$MAPPING_FILE"
    --database="$db"
    --host="$host"
    --port="$port"
    --username="$user"
  )

  case "$mode" in
    create)
      osm2pgsql_args+=(--create)
      if oi_is_truthy "$drop_middle"; then
        osm2pgsql_args+=(--drop)
      fi
      ;;
    append)
      osm2pgsql_args+=(--append)
      ;;
    *)
      oi_die "unexpected import mode: $mode"
      ;;
  esac

  if [[ -n "${OSM2PGSQL_NUM_PROCESSES:-}" ]]; then
    osm2pgsql_args+=(--number-processes="$OSM2PGSQL_NUM_PROCESSES")
  fi

  oi_log "Running osm2pgsql import ($label)"
  echo "  mode: $mode"
  echo "  input: $input_pbf"
  echo "  mapping: $MAPPING_FILE"
  echo "  flatnodes: $flatnodes_path"
  echo "  target: postgresql://${user}@${host}:${port}/${db}"

  if ! PGPASSWORD="$pass" "$OSM2PGSQL_BIN" "${osm2pgsql_args[@]}" "$input_pbf"; then
    if [[ "$mode" == "append" ]]; then
      cat >&2 <<'HINT'
append mode failed. If this is a first-time bootstrap with no existing canonical tables,
rerun with: OSM2PGSQL_MODE=create
HINT
    fi
    return 1
  fi
}

run_import "canonical" "$OSM_DB_HOST" "$OSM_DB_PORT" "$OSM_DB_NAME" "$OSM_DB_USER" "$OSM_DB_PASSWORD" "$IMPORT_PBF_FILE" "$IMPORT_MODE"

if [[ -n "$RESOLVED_IMPORT_PBF_OUT" ]]; then
  printf '%s' "$IMPORT_PBF_FILE" > "$RESOLVED_IMPORT_PBF_OUT"
fi

echo "osm2pgsql import complete (mode=$IMPORT_MODE)"
