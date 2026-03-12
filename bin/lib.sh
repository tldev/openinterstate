#!/usr/bin/env bash

set -euo pipefail

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$BIN_DIR/.." && pwd)"
DEFAULT_ENV_FILE="$REPO_ROOT/.env"

oi_log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >&2
}

oi_die() {
  echo "ERROR: $*" >&2
  exit 1
}

oi_require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    oi_die "missing required command: $cmd"
  fi
}

oi_is_truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|y|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

oi_load_env() {
  local env_file="${1:-$DEFAULT_ENV_FILE}"
  local line key idx
  local -a preset_keys=()
  local -a preset_vals=()

  if [[ ! -f "$env_file" ]]; then
    return 0
  fi

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    if [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)= ]]; then
      key="${BASH_REMATCH[1]}"
      if [[ -n "${!key+x}" ]]; then
        preset_keys+=("$key")
        preset_vals+=("${!key}")
      fi
    fi
  done < "$env_file"

  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a

  for idx in "${!preset_keys[@]}"; do
    key="${preset_keys[$idx]}"
    export "$key=${preset_vals[$idx]}"
  done
}

oi_set_defaults() {
  OI_DB_PORT="${OI_DB_PORT:-5434}"
  OI_DEFAULT_US_PBF_URL="${OI_DEFAULT_US_PBF_URL:-https://download.geofabrik.de/north-america/us-latest.osm.pbf}"
  OSM2PGSQL_MODE="${OSM2PGSQL_MODE:-auto}"
  OSM2PGSQL_DROP_MIDDLE="${OSM2PGSQL_DROP_MIDDLE:-false}"
  OI_ALLOW_CANONICAL_RESET="${OI_ALLOW_CANONICAL_RESET:-false}"

  OI_DB_NAME="${OI_DB_NAME:-osm}"
  OI_DB_USER="${OI_DB_USER:-osm}"
  OI_DB_PASSWORD="${OI_DB_PASSWORD:-osm_dev}"
  OI_DB_SERVICE="${OI_DB_SERVICE:-db}"
  OI_DB_HOST="${OI_DB_HOST:-db}"
  OI_DB_CONTAINER_PORT="${OI_DB_CONTAINER_PORT:-5432}"
  PRODUCT_DB_URL="${PRODUCT_DB_URL:-postgres://${OI_DB_USER}:${OI_DB_PASSWORD}@${OI_DB_HOST}:${OI_DB_CONTAINER_PORT}/${OI_DB_NAME}}"

  OI_DATA_ROOT="$(oi_abs_path "${OI_DATA_ROOT:-$REPO_ROOT/.data}")"
  OI_POSTGRES_DIR="$(oi_abs_path "${OI_POSTGRES_DIR:-$OI_DATA_ROOT/postgres/db}")"
  OI_FLATNODES_DIR="$(oi_abs_path "${OI_FLATNODES_DIR:-$OI_DATA_ROOT/flatnodes}")"
  OI_DOWNLOAD_DIR="$(oi_abs_path "${OI_DOWNLOAD_DIR:-$OI_DATA_ROOT/downloads}")"
  OI_FILTERED_DIR="$(oi_abs_path "${OI_FILTERED_DIR:-$OI_DATA_ROOT/filtered}")"
  OI_RELEASE_DIR="$(oi_abs_path "${OI_RELEASE_DIR:-${OI_BUILD_DIR:-$OI_DATA_ROOT/releases}}")"
  OI_BUILD_DIR="$OI_RELEASE_DIR"

  export OI_DB_PORT
  export OI_DATA_ROOT OI_POSTGRES_DIR OI_FLATNODES_DIR OI_DOWNLOAD_DIR OI_FILTERED_DIR OI_RELEASE_DIR OI_BUILD_DIR
}

oi_prepare_dirs() {
  mkdir -p \
    "$OI_DATA_ROOT" \
    "$OI_POSTGRES_DIR" \
    "$OI_FLATNODES_DIR" \
    "$OI_DOWNLOAD_DIR" \
    "$OI_FILTERED_DIR" \
    "$OI_RELEASE_DIR"
}

oi_compose_cmd() {
  local env_file="${1:-$DEFAULT_ENV_FILE}"
  OI_COMPOSE_CMD=(docker compose)
  if [[ -f "$env_file" ]]; then
    OI_COMPOSE_CMD+=(--env-file "$env_file")
  fi
}

oi_runner() {
  "${OI_COMPOSE_CMD[@]}" run --use-aliases --rm -T runner "$@"
}

oi_db_exec() {
  "${OI_COMPOSE_CMD[@]}" exec -T "$OI_DB_SERVICE" "$@"
}

oi_db_query() {
  oi_db_exec psql -U "$OI_DB_USER" -d "$OI_DB_NAME" -Atc "$1"
}

oi_db_up() {
  oi_log "Starting local PostGIS"
  "${OI_COMPOSE_CMD[@]}" up -d "$OI_DB_SERVICE"
}

oi_db_down() {
  oi_log "Stopping local services"
  "${OI_COMPOSE_CMD[@]}" down --remove-orphans
}

oi_wait_for_db() {
  until oi_runner env PGPASSWORD="$OI_DB_PASSWORD" \
    psql \
      -h "$OI_DB_HOST" \
      -p "$OI_DB_CONTAINER_PORT" \
      -U "$OI_DB_USER" \
      -d "$OI_DB_NAME" \
      -Atc "SELECT 1" >/dev/null 2>&1
  do
    sleep 2
  done
}

oi_abs_path() {
  local path="$1"
  local dir base suffix=""
  if [[ "$path" != /* ]]; then
    path="$PWD/$path"
  fi

  dir="$(dirname "$path")"
  base="$(basename "$path")"
  while [[ ! -d "$dir" && "$dir" != "/" ]]; do
    suffix="/$(basename "$dir")$suffix"
    dir="$(dirname "$dir")"
  done
  dir="$(cd "$dir" && pwd)"

  if [[ -n "$suffix" ]]; then
    printf '%s%s/%s\n' "$dir" "$suffix" "$base"
  else
    printf '%s/%s\n' "$dir" "$base"
  fi
}

oi_path_is_under() {
  local path="$1"
  local root="$2"
  [[ "$path" == "$root" || "$path" == "$root/"* ]]
}

oi_path_is_in_repo() {
  oi_path_is_under "$1" "$REPO_ROOT"
}

oi_path_is_in_data_root() {
  oi_path_is_under "$1" "$OI_DATA_ROOT"
}

oi_path_is_in_release_root() {
  oi_path_is_under "$1" "$OI_RELEASE_DIR"
}

oi_path_is_managed() {
  local path="$1"
  oi_path_is_in_repo "$path" || oi_path_is_in_data_root "$path" || oi_path_is_in_release_root "$path"
}

oi_managed_path() {
  local path
  path="$(oi_abs_path "$1")"
  if ! oi_path_is_managed "$path"; then
    oi_die "path must live inside the repository or data directory: $path"
  fi
  printf '%s\n' "$path"
}

oi_stage_input_file() {
  local source="$1"
  local abs_source staged_path

  abs_source="$(oi_abs_path "$source")"
  if oi_path_is_managed "$abs_source"; then
    printf '%s\n' "$abs_source"
    return 0
  fi

  staged_path="$OI_DOWNLOAD_DIR/$(basename "$abs_source")"
  mkdir -p "$OI_DOWNLOAD_DIR"
  if [[ ! -f "$staged_path" || "$abs_source" -nt "$staged_path" ]]; then
    oi_log "Staging external input into managed downloads"
    echo "  source: $abs_source" >&2
    echo "  staged: $staged_path" >&2
    cp "$abs_source" "$staged_path"
  fi

  printf '%s\n' "$staged_path"
}

oi_container_path() {
  local host_path rel_path
  host_path="$(oi_managed_path "$1")"

  if [[ "$host_path" == "$REPO_ROOT" ]]; then
    printf '/workspace\n'
    return 0
  fi

  if oi_path_is_in_repo "$host_path"; then
    rel_path="${host_path#$REPO_ROOT/}"
    printf '/workspace/%s\n' "$rel_path"
    return 0
  fi

  if [[ "$host_path" == "$OI_DATA_ROOT" ]]; then
    printf '/data\n'
    return 0
  fi

  if [[ "$host_path" == "$OI_RELEASE_DIR" ]]; then
    printf '/releases\n'
    return 0
  fi

  if oi_path_is_in_release_root "$host_path" && ! oi_path_is_in_data_root "$host_path"; then
    rel_path="${host_path#$OI_RELEASE_DIR/}"
    printf '/releases/%s\n' "$rel_path"
    return 0
  fi

  rel_path="${host_path#$OI_DATA_ROOT/}"
  printf '/data/%s\n' "$rel_path"
}

oi_guard_no_reachability_clears() {
  local sql_file="$1"
  local pattern

  for pattern in \
    'TRUNCATE[[:space:]]+[^;]*exit_poi_reachability' \
    'DELETE[[:space:]]+FROM[[:space:]]+exit_poi_reachability' \
    'DROP[[:space:]]+TABLE[[:space:]]+[^;]*exit_poi_reachability'
  do
    if rg -n -i -e "$pattern" "$sql_file" >/dev/null 2>&1; then
      oi_die "guardrail violation in $(basename "$sql_file"): reachability clears are forbidden"
    fi
  done
}

oi_download_pbf() {
  local source_url="$1"
  local output_path="${2:-}"
  local resolved_output

  if [[ -z "$output_path" ]]; then
    resolved_output="$OI_DOWNLOAD_DIR/$(basename "$source_url")"
  else
    resolved_output="$(oi_managed_path "$output_path")"
  fi

  mkdir -p "$(dirname "$resolved_output")"

  oi_log "Downloading source PBF"
  echo "  url: $source_url" >&2
  echo "  output: $resolved_output" >&2

  oi_runner curl -L --fail --progress-bar "$source_url" -o "$(oi_container_path "$resolved_output")"
  printf '%s\n' "$resolved_output"
}

oi_filter_pbf() {
  local input_pbf="$1"
  local output_pbf="$2"
  local force="${3:-false}"
  local output_tmp

  input_pbf="$(oi_stage_input_file "$input_pbf")"
  output_pbf="$(oi_managed_path "$output_pbf")"

  if [[ ! -f "$input_pbf" ]]; then
    oi_die "input PBF not found: $input_pbf"
  fi

  mkdir -p "$(dirname "$output_pbf")"
  if [[ "$force" != true && -s "$output_pbf" && ! "$input_pbf" -nt "$output_pbf" ]]; then
    oi_log "Skipping canonical filter; output is current"
    echo "  output: $output_pbf" >&2
    printf '%s\n' "$output_pbf"
    return 0
  fi

  if [[ "$output_pbf" == *.osm.pbf ]]; then
    output_tmp="${output_pbf%.osm.pbf}.tmp.$$.osm.pbf"
  else
    output_tmp="${output_pbf}.tmp.$$"
  fi

  oi_log "Filtering canonical import PBF"
  echo "  input: $input_pbf" >&2
  echo "  output: $output_pbf" >&2

  oi_runner osmium tags-filter \
    "$(oi_container_path "$input_pbf")" \
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
    -o "$(oi_container_path "$output_tmp")"

  mv "$output_tmp" "$output_pbf"
  printf '%s\n' "$output_pbf"
}

oi_canonical_tables_exist() {
  local exists
  exists="$(oi_db_query "SELECT CASE WHEN to_regclass('public.osm2pgsql_v2_highways') IS NULL THEN 0 ELSE 1 END;" 2>/dev/null || true)"
  [[ "$exists" == "1" ]]
}

oi_canonical_db_updatable() {
  local raw
  raw="$(oi_db_query "SELECT value FROM osm2pgsql_properties WHERE property = 'updatable' LIMIT 1;" 2>/dev/null || true)"
  case "$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')" in
    true|1|yes|y|on) return 0 ;;
    false|0|no|n|off) return 1 ;;
    *) return 2 ;;
  esac
}

oi_assert_canonical_import_ready() {
  local table_exists has_rows

  table_exists="$(oi_db_query "SELECT CASE WHEN to_regclass('public.osm2pgsql_v2_highways') IS NULL THEN 0 ELSE 1 END;" 2>/dev/null || true)"
  if [[ "$table_exists" != "1" ]]; then
    oi_die "canonical osm2pgsql import did not produce osm2pgsql_v2_highways"
  fi

  has_rows="$(oi_db_query "SELECT CASE WHEN EXISTS (SELECT 1 FROM osm2pgsql_v2_highways LIMIT 1) THEN 1 ELSE 0 END;" 2>/dev/null || true)"
  if [[ "$has_rows" != "1" ]]; then
    oi_die "canonical osm2pgsql import produced an empty highway table"
  fi
}

oi_resolve_import_mode() {
  local requested_mode="${OSM2PGSQL_MODE:-auto}"
  local canonical_state="unknown"
  local detected_mode="append"

  case "$requested_mode" in
    auto|append|create)
      ;;
    *)
      oi_die "invalid OSM2PGSQL_MODE=$requested_mode (expected auto|append|create)"
      ;;
  esac

  if oi_canonical_tables_exist; then
    canonical_state="present"
  else
    canonical_state="missing"
  fi

  case "$requested_mode" in
    append)
      detected_mode="append"
      ;;
    create)
      detected_mode="create"
      ;;
    auto)
      if [[ "$canonical_state" == "missing" ]]; then
        detected_mode="create"
      else
        detected_mode="append"
      fi
      ;;
  esac

  if [[ "$detected_mode" == "create" && "$canonical_state" == "present" ]]; then
    if ! oi_is_truthy "$OI_ALLOW_CANONICAL_RESET"; then
      oi_die "refusing canonical reset: set OI_ALLOW_CANONICAL_RESET=true to permit create mode"
    fi
  fi

  printf '%s\n' "$detected_mode"
}

oi_import_canonical() {
  local source_pbf="$1"
  local prefilter="${2:-true}"
  local force_prefilter="${3:-false}"
  local import_pbf pbf_basename pbf_stem filtered_output import_mode
  local flatnodes_path drop_middle mapping_file
  local -a osm2pgsql_args=()

  source_pbf="$(oi_stage_input_file "$source_pbf")"
  if [[ ! -f "$source_pbf" ]]; then
    oi_die "PBF file not found: $source_pbf"
  fi

  import_pbf="$source_pbf"
  if [[ "$prefilter" == true ]]; then
    pbf_basename="$(basename "$source_pbf")"
    pbf_stem="${pbf_basename%.osm.pbf}"
    if [[ "$pbf_stem" == "$pbf_basename" ]]; then
      pbf_stem="${pbf_basename%.*}"
    fi
    filtered_output="$OI_FILTERED_DIR/${pbf_stem}.canonical-filtered.osm.pbf"
    import_pbf="$(oi_filter_pbf "$source_pbf" "$filtered_output" "$force_prefilter")"
  fi

  import_mode="$(oi_resolve_import_mode)"
  if [[ "$import_mode" == "append" ]]; then
    if oi_canonical_db_updatable; then
      :
    else
      case "$?" in
        1)
          echo "Warning: canonical osm2pgsql DB is not updatable." >&2
          echo "Skipping canonical import to preserve existing OSM data." >&2
          printf '%s\n' "$import_pbf"
          return 0
          ;;
      esac
    fi
  fi

  mapping_file="$REPO_ROOT/schema/osm2pgsql/openinterstate.lua"
  pbf_basename="$(basename "$import_pbf")"
  pbf_stem="${pbf_basename%.osm.pbf}"
  if [[ "$pbf_stem" == "$pbf_basename" ]]; then
    pbf_stem="${pbf_basename%.*}"
  fi
  flatnodes_path="$OI_FLATNODES_DIR/${OI_DB_NAME}_${pbf_stem}.flatnodes.bin"
  drop_middle="${OSM2PGSQL_DROP_MIDDLE:-false}"

  osm2pgsql_args=(
    --slim
    --cache=0
    --flat-nodes="$(oi_container_path "$flatnodes_path")"
    --output=flex
    --style="$(oi_container_path "$mapping_file")"
    --database="$OI_DB_NAME"
    --host="$OI_DB_HOST"
    --port="$OI_DB_CONTAINER_PORT"
    --username="$OI_DB_USER"
  )

  case "$import_mode" in
    create)
      osm2pgsql_args+=(--create)
      if oi_is_truthy "$drop_middle"; then
        osm2pgsql_args+=(--drop)
      fi
      ;;
    append)
      osm2pgsql_args+=(--append)
      ;;
  esac

  oi_log "Running canonical osm2pgsql import"
  echo "  mode: $import_mode" >&2
  echo "  input: $import_pbf" >&2
  echo "  mapping: $mapping_file" >&2

  oi_runner env PGPASSWORD="$OI_DB_PASSWORD" \
    osm2pgsql "${osm2pgsql_args[@]}" "$(oi_container_path "$import_pbf")"
  oi_assert_canonical_import_ready

  printf '%s\n' "$import_pbf"
}

oi_apply_derive() {
  local derive_file="$REPO_ROOT/schema/derive.sql"

  oi_guard_no_reachability_clears "$derive_file"

  oi_log "Applying deterministic SQL projection"
  oi_db_exec psql -U "$OI_DB_USER" -d "$OI_DB_NAME" -v ON_ERROR_STOP=1 < "$derive_file"

  oi_log "Building graph, corridors, and reference routes"
  oi_runner cargo run --release -p openinterstate-derive -- \
    --database-url "$PRODUCT_DB_URL" \
    all
}

oi_export_release() {
  local release_id="$1"
  local source_pbf="$2"
  local import_pbf="${3:-$2}"
  local source_url="${4:-}"
  local output_root="${5:-$OI_RELEASE_DIR}"
  local output_dir archive_path
  local -a export_args

  source_pbf="$(oi_stage_input_file "$source_pbf")"
  import_pbf="$(oi_stage_input_file "$import_pbf")"
  output_root="$(oi_managed_path "$output_root")"
  output_dir="$output_root/$release_id"
  archive_path="$output_root/openinterstate-$release_id.tar.gz"

  mkdir -p "$output_root"

  oi_log "Exporting release artifacts"
  echo "  release id: $release_id" >&2
  echo "  output dir: $output_dir" >&2

  export_args=(
    python3
    /workspace/tooling/export_release.py
    --database-url "$PRODUCT_DB_URL"
    --release-id "$release_id"
    --output-dir "$(oi_container_path "$output_dir")"
    --source-pbf-file "$(oi_container_path "$source_pbf")"
    --import-pbf-file "$(oi_container_path "$import_pbf")"
  )
  if [[ -n "$source_url" ]]; then
    export_args+=(--source-url "$source_url")
  fi

  oi_runner "${export_args[@]}"

  find "$output_dir" \
    \( -name '.DS_Store' -o -name '._*' \) \
    -type f \
    -delete

  oi_runner tar \
    --exclude='.DS_Store' \
    --exclude='._*' \
    -C "$(oi_container_path "$output_root")" \
    -czf "$(oi_container_path "$archive_path")" \
    "$release_id"
}

oi_publish_release() {
  local release_id="$1"
  local output_root="${2:-$OI_RELEASE_DIR}"
  local release_dir archive_path manifest_path checksums_path source_lineage_path
  local release_notes

  oi_require_cmd gh
  output_root="$(oi_managed_path "$output_root")"
  release_dir="$output_root/$release_id"
  archive_path="$output_root/openinterstate-$release_id.tar.gz"
  manifest_path="$release_dir/manifest.json"
  checksums_path="$release_dir/checksums.txt"
  source_lineage_path="$release_dir/source_lineage.json"

  for path in "$release_dir" "$archive_path" "$manifest_path" "$checksums_path" "$source_lineage_path"; do
    [[ -e "$path" ]] || oi_die "missing publish artifact: $path"
  done

  release_notes="Rebuilt from the raw U.S. OSM PBF using the standalone OpenInterstate pipeline. See manifest.json and source_lineage.json for raw-source and imported-filter lineage, including SHA-256 hashes."

  if gh release view "$release_id" --repo tldev/openinterstate >/dev/null 2>&1; then
    gh release upload "$release_id" \
      "$archive_path" \
      "$manifest_path" \
      "$checksums_path" \
      "$source_lineage_path" \
      --repo tldev/openinterstate \
      --clobber
    gh release edit "$release_id" \
      --repo tldev/openinterstate \
      --title "$release_id" \
      --notes "$release_notes"
  else
    gh release create "$release_id" \
      "$archive_path" \
      "$manifest_path" \
      "$checksums_path" \
      "$source_lineage_path" \
      --repo tldev/openinterstate \
      --title "$release_id" \
      --notes "$release_notes"
  fi
}
