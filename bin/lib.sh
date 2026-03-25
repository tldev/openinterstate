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

oi_hash_stdin() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
  else
    sha256sum | awk '{print $1}'
  fi
}

oi_hash_text() {
  printf '%s' "$1" | oi_hash_stdin
}

oi_hash_files() {
  if [[ $# -eq 0 ]]; then
    oi_die "oi_hash_files requires at least one path"
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$@" | oi_hash_stdin
  else
    sha256sum "$@" | oi_hash_stdin
  fi
}

oi_hash_file_sha256() {
  local path="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
  else
    sha256sum "$path" | awk '{print $1}'
  fi
}

oi_file_signature() {
  local path="$1"
  if stat -c '%n|%s|%Y' "$path" >/dev/null 2>&1; then
    stat -c '%n|%s|%Y' "$path"
  else
    stat -f '%N|%z|%m' "$path"
  fi
}

oi_file_size_bytes() {
  local path="$1"
  if stat -c '%s' "$path" >/dev/null 2>&1; then
    stat -c '%s' "$path"
  else
    stat -f '%z' "$path"
  fi
}

oi_state_file() {
  local scope="$1"
  local key="$2"
  printf '%s/%s-%s.state\n' "$OI_STATE_DIR" "$scope" "$(oi_hash_text "$key")"
}

oi_state_read() {
  local path="$1"
  local key="$2"

  [[ -f "$path" ]] || return 1
  awk -F= -v wanted="$key" '
    $1 == wanted {
      sub($1 "=", "", $0)
      print $0
      exit 0
    }
  ' "$path"
}

oi_state_write() {
  local path="$1"
  shift
  local tmp_path="${path}.tmp.$$"

  mkdir -p "$(dirname "$path")"
  : > "$tmp_path"
  while [[ $# -gt 1 ]]; do
    printf '%s=%s\n' "$1" "$2" >> "$tmp_path"
    shift 2
  done

  mv "$tmp_path" "$path"
}

oi_cleanup_unused_flatnodes() {
  local flatnodes_path="$1"

  if [[ -f "$flatnodes_path" ]]; then
    oi_log "Removing stale flatnodes cache"
    echo "  path: $flatnodes_path" >&2
    rm -f "$flatnodes_path"
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

oi_export_path_vars() {
  export OI_DATA_PARENT OI_SOURCE_CACHE_DIR OI_INDEX_DIR OI_PARENT_CACHE_DIR OI_WORKSPACES_DIR
  export OI_DATA_ROOT OI_POSTGRES_DIR OI_FLATNODES_DIR OI_DOWNLOAD_DIR OI_FILTERED_DIR
  export OI_STATE_DIR OI_CACHE_DIR OI_CARGO_REGISTRY_DIR OI_CARGO_GIT_DIR OI_CARGO_TARGET_DIR
  export OI_RELEASE_DIR OI_BUILD_DIR OI_PBF_SHA256
}

oi_configure_data_root() {
  local data_root="$1"
  local release_root

  OI_DATA_ROOT="$(oi_abs_path "$data_root")"
  OI_POSTGRES_DIR="$(oi_abs_path "$OI_DATA_ROOT/postgres/db")"
  OI_FLATNODES_DIR="$(oi_abs_path "$OI_DATA_ROOT/flatnodes")"
  OI_DOWNLOAD_DIR="$(oi_abs_path "$OI_DATA_ROOT/downloads")"
  OI_FILTERED_DIR="$(oi_abs_path "$OI_DATA_ROOT/filtered")"
  OI_STATE_DIR="$(oi_abs_path "$OI_DATA_ROOT/state")"
  OI_CACHE_DIR="$(oi_abs_path "$OI_DATA_ROOT/cache")"

  if [[ "$OI_RELEASE_DIR_IS_EXPLICIT" == true ]]; then
    release_root="$OI_RELEASE_DIR"
  elif [[ "$OI_BUILD_DIR_IS_EXPLICIT" == true ]]; then
    release_root="$OI_BUILD_DIR"
  else
    release_root="$OI_DATA_ROOT/releases"
  fi
  OI_RELEASE_DIR="$(oi_abs_path "$release_root")"
  OI_BUILD_DIR="$OI_RELEASE_DIR"

  oi_export_path_vars
}

oi_set_defaults() {
  local data_root_was_set=false
  local release_dir_was_set=false
  local build_dir_was_set=false

  if [[ -n "${OI_DATA_ROOT+x}" ]]; then
    data_root_was_set=true
  fi
  if [[ -n "${OI_RELEASE_DIR+x}" ]]; then
    release_dir_was_set=true
  fi
  if [[ -n "${OI_BUILD_DIR+x}" ]]; then
    build_dir_was_set=true
  fi

  OI_DB_PORT="${OI_DB_PORT:-5434}"
  OI_DEFAULT_US_PBF_URL="${OI_DEFAULT_US_PBF_URL:-https://download.geofabrik.de/north-america/us-latest.osm.pbf}"
  OSM2PGSQL_MODE="${OSM2PGSQL_MODE:-auto}"
  OSM2PGSQL_DROP_MIDDLE="${OSM2PGSQL_DROP_MIDDLE:-false}"
  OI_ALLOW_CANONICAL_RESET="${OI_ALLOW_CANONICAL_RESET:-false}"
  OI_FLATNODES_MODE="${OI_FLATNODES_MODE:-auto}"
  OI_FLATNODES_AUTO_MAX_PBF_MB="${OI_FLATNODES_AUTO_MAX_PBF_MB:-1024}"
  OI_IMPORT_CACHE_MB="${OI_IMPORT_CACHE_MB:-2048}"

  OI_DB_NAME="${OI_DB_NAME:-osm}"
  OI_DB_USER="${OI_DB_USER:-osm}"
  OI_DB_PASSWORD="${OI_DB_PASSWORD:-osm_dev}"
  OI_DB_SERVICE="${OI_DB_SERVICE:-db}"
  OI_DB_HOST="${OI_DB_HOST:-db}"
  OI_DB_CONTAINER_PORT="${OI_DB_CONTAINER_PORT:-5432}"
  PRODUCT_DB_URL="${PRODUCT_DB_URL:-postgres://${OI_DB_USER}:${OI_DB_PASSWORD}@${OI_DB_HOST}:${OI_DB_CONTAINER_PORT}/${OI_DB_NAME}}"

  OI_DATA_ROOT_IS_EXPLICIT="$data_root_was_set"
  OI_RELEASE_DIR_IS_EXPLICIT="$release_dir_was_set"
  OI_BUILD_DIR_IS_EXPLICIT="$build_dir_was_set"
  if [[ "$OI_DATA_ROOT_IS_EXPLICIT" == true ]]; then
    OI_DATA_PARENT="$(oi_abs_path "${OI_DATA_PARENT:-$OI_DATA_ROOT}")"
  else
    OI_DATA_PARENT="$(oi_abs_path "${OI_DATA_PARENT:-/Volumes/goose-drive/openinterstate}")"
  fi
  OI_SOURCE_CACHE_DIR="$(oi_abs_path "${OI_SOURCE_CACHE_DIR:-$OI_DATA_PARENT/source-cache}")"
  OI_INDEX_DIR="$(oi_abs_path "${OI_INDEX_DIR:-$OI_DATA_PARENT/index}")"
  OI_PARENT_CACHE_DIR="$(oi_abs_path "${OI_PARENT_CACHE_DIR:-$OI_DATA_PARENT/cache}")"
  OI_WORKSPACES_DIR="$(oi_abs_path "${OI_WORKSPACES_DIR:-$OI_DATA_PARENT/workspaces/pbf-sha256}")"
  OI_PBF_SHA256="${OI_PBF_SHA256:-}"
  OI_CARGO_REGISTRY_DIR="$(oi_abs_path "${OI_CARGO_REGISTRY_DIR:-$OI_PARENT_CACHE_DIR/cargo/registry}")"
  OI_CARGO_GIT_DIR="$(oi_abs_path "${OI_CARGO_GIT_DIR:-$OI_PARENT_CACHE_DIR/cargo/git}")"
  OI_CARGO_TARGET_DIR="$(oi_abs_path "${OI_CARGO_TARGET_DIR:-$OI_PARENT_CACHE_DIR/cargo/target}")"

  if [[ "$OI_DATA_ROOT_IS_EXPLICIT" == true ]]; then
    oi_configure_data_root "$OI_DATA_ROOT"
  else
    oi_configure_data_root "$OI_DATA_PARENT"
  fi

  export OI_DB_PORT
  export OI_DATA_ROOT_IS_EXPLICIT OI_RELEASE_DIR_IS_EXPLICIT OI_BUILD_DIR_IS_EXPLICIT
  export OI_FLATNODES_MODE OI_FLATNODES_AUTO_MAX_PBF_MB OI_IMPORT_CACHE_MB
}

oi_prepare_parent_dirs() {
  mkdir -p \
    "$OI_DATA_PARENT" \
    "$OI_SOURCE_CACHE_DIR" \
    "$OI_INDEX_DIR" \
    "$OI_PARENT_CACHE_DIR" \
    "$OI_WORKSPACES_DIR" \
    "$OI_CARGO_REGISTRY_DIR" \
    "$OI_CARGO_GIT_DIR" \
    "$OI_CARGO_TARGET_DIR"

  if [[ "$OI_RELEASE_DIR_IS_EXPLICIT" == true ]]; then
    mkdir -p "$OI_RELEASE_DIR"
  fi
}

oi_prepare_dirs() {
  oi_prepare_parent_dirs
  mkdir -p \
    "$OI_DATA_ROOT" \
    "$OI_POSTGRES_DIR" \
    "$OI_FLATNODES_DIR" \
    "$OI_DOWNLOAD_DIR" \
    "$OI_FILTERED_DIR" \
    "$OI_STATE_DIR" \
    "$OI_CACHE_DIR" \
    "$OI_CARGO_REGISTRY_DIR" \
    "$OI_CARGO_GIT_DIR" \
    "$OI_CARGO_TARGET_DIR" \
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

oi_path_is_in_data_parent() {
  oi_path_is_under "$1" "$OI_DATA_PARENT"
}

oi_path_is_in_release_root() {
  oi_path_is_under "$1" "$OI_RELEASE_DIR"
}

oi_path_is_managed() {
  local path="$1"
  oi_path_is_in_repo "$path" || oi_path_is_in_data_root "$path" || oi_path_is_in_data_parent "$path" || oi_path_is_in_release_root "$path"
}

oi_managed_path() {
  local path
  path="$(oi_abs_path "$1")"
  if ! oi_path_is_managed "$path"; then
    oi_die "path must live inside the repository or managed data parent: $path"
  fi
  printf '%s\n' "$path"
}

oi_parent_state_file() {
  local scope="$1"
  local key="$2"
  printf '%s/%s-%s.state\n' "$OI_INDEX_DIR" "$scope" "$(oi_hash_text "$key")"
}

oi_source_pbf_sha256() {
  local source_pbf="$1"
  local abs_source signature state_file cached_signature cached_sha256

  abs_source="$(oi_abs_path "$source_pbf")"
  [[ -f "$abs_source" ]] || oi_die "source PBF not found: $abs_source"

  signature="$(oi_file_signature "$abs_source")"
  state_file="$(oi_parent_state_file pbf-sha256 "$signature")"
  cached_signature="$(oi_state_read "$state_file" signature 2>/dev/null || true)"
  cached_sha256="$(oi_state_read "$state_file" sha256 2>/dev/null || true)"
  if [[ "$cached_signature" == "$signature" && ${#cached_sha256} -eq 64 ]]; then
    printf '%s\n' "$cached_sha256"
    return 0
  fi

  oi_log "Hashing source PBF to select workspace"
  echo "  source: $abs_source" >&2
  cached_sha256="$(oi_hash_file_sha256 "$abs_source")"
  oi_state_write "$state_file" \
    signature "$signature" \
    source_pbf "$abs_source" \
    sha256 "$cached_sha256" \
    completed_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf '%s\n' "$cached_sha256"
}

oi_workspace_root_for_sha256() {
  local sha256="$1"
  printf '%s/%s\n' "$OI_WORKSPACES_DIR" "$sha256"
}

oi_json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  printf '%s' "$value"
}

oi_write_workspace_metadata() {
  local source_pbf="$1"
  local source_url="${2:-}"
  local metadata_file="$OI_DATA_ROOT/workspace.json"
  local metadata_tmp="${metadata_file}.tmp.$$"

  mkdir -p "$OI_DATA_ROOT"
  {
    printf '{\n'
    printf '  "layout": "pbf-sha256",\n'
    printf '  "pbf_sha256": "%s",\n' "$(oi_json_escape "$OI_PBF_SHA256")"
    printf '  "data_parent": "%s",\n' "$(oi_json_escape "$OI_DATA_PARENT")"
    printf '  "workspace_root": "%s",\n' "$(oi_json_escape "$OI_DATA_ROOT")"
    printf '  "release_root": "%s",\n' "$(oi_json_escape "$OI_RELEASE_DIR")"
    printf '  "source_pbf": "%s"' "$(oi_json_escape "$source_pbf")"
    if [[ -n "$source_url" ]]; then
      printf ',\n  "source_url": "%s"\n' "$(oi_json_escape "$source_url")"
    else
      printf '\n'
    fi
    printf '}\n'
  } > "$metadata_tmp"
  mv "$metadata_tmp" "$metadata_file"
}

oi_activate_workspace_for_source_pbf() {
  local source_pbf="$1"
  local source_url="${2:-}"
  local abs_source source_sha workspace_root

  abs_source="$(oi_abs_path "$source_pbf")"
  [[ -f "$abs_source" ]] || oi_die "source PBF not found: $abs_source"

  # A given source PBF always resolves to the same workspace path.
  source_sha="$(oi_source_pbf_sha256 "$abs_source")"
  workspace_root="$(oi_workspace_root_for_sha256 "$source_sha")"

  OI_PBF_SHA256="$source_sha"
  oi_configure_data_root "$workspace_root"
  oi_prepare_dirs
  oi_write_workspace_metadata "$abs_source" "$source_url"
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

  if oi_path_is_in_data_root "$host_path"; then
    rel_path="${host_path#$OI_DATA_ROOT/}"
    printf '/data/%s\n' "$rel_path"
    return 0
  fi

  if [[ "$host_path" == "$OI_DATA_PARENT" ]]; then
    printf '/managed\n'
    return 0
  fi

  if oi_path_is_in_data_parent "$host_path"; then
    rel_path="${host_path#$OI_DATA_PARENT/}"
    printf '/managed/%s\n' "$rel_path"
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
  local -a curl_args=()

  if [[ -z "$output_path" ]]; then
    resolved_output="$OI_SOURCE_CACHE_DIR/$(basename "$source_url")"
  else
    resolved_output="$(oi_managed_path "$output_path")"
  fi

  mkdir -p "$(dirname "$resolved_output")"

  oi_log "Resolving source PBF"
  echo "  url: $source_url" >&2
  echo "  output: $resolved_output" >&2

  curl_args=(
    -L
    --fail
    --progress-bar
    --remote-time
  )
  if [[ -f "$resolved_output" ]]; then
    curl_args+=(-z "$(oi_container_path "$resolved_output")")
  fi

  oi_runner curl "${curl_args[@]}" "$source_url" -o "$(oi_container_path "$resolved_output")" >&2 || return $?
  printf '%s\n' "$resolved_output"
}

oi_canonical_filter_args() {
  cat <<'EOF'
n/highway=motorway_junction
n/amenity=fuel,restaurant,fast_food,cafe,charging_station
n/tourism=hotel,motel
n/shop=gas
n/cuisine
n/highway=rest_area,services
w/highway=construction
w/highway=motorway,motorway_link,trunk,trunk_link,rest_area,services
w/amenity=fuel,restaurant,fast_food,cafe,charging_station
w/tourism=hotel,motel
w/shop=gas
w/cuisine
EOF
}

oi_filter_pbf() {
  local input_pbf="$1"
  local output_pbf="$2"
  local force="${3:-false}"
  local output_tmp
  local state_file expected_signature
  local -a filter_args=()

  input_pbf="$(oi_stage_input_file "$input_pbf")"
  output_pbf="$(oi_managed_path "$output_pbf")"

  if [[ ! -f "$input_pbf" ]]; then
    oi_die "input PBF not found: $input_pbf"
  fi

  mkdir -p "$(dirname "$output_pbf")"
  while IFS= read -r line; do
    filter_args+=("$line")
  done < <(oi_canonical_filter_args)
  state_file="$(oi_state_file filter "$output_pbf")"
  expected_signature="$(
    {
      oi_file_signature "$input_pbf"
      printf '%s\n' "${filter_args[@]}"
    } | oi_hash_stdin
  )"
  if [[ "$force" != true && -s "$output_pbf" ]]; then
    if [[ "$(oi_state_read "$state_file" signature 2>/dev/null || true)" == "$expected_signature" ]] \
      && [[ ! "$input_pbf" -nt "$output_pbf" ]]; then
      oi_log "Skipping canonical filter; output is current"
      echo "  output: $output_pbf" >&2
      oi_state_write "$state_file" \
        signature "$expected_signature" \
        input_pbf "$input_pbf" \
        output_pbf "$output_pbf" \
        completed_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
      printf '%s\n' "$output_pbf"
      return 0
    fi
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
    "${filter_args[@]}" \
    --overwrite \
    -o "$(oi_container_path "$output_tmp")" >&2 || return $?

  mv "$output_tmp" "$output_pbf"
  oi_state_write "$state_file" \
    signature "$expected_signature" \
    input_pbf "$input_pbf" \
    output_pbf "$output_pbf" \
    completed_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
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

oi_derive_outputs_ready() {
  local exists

  exists="$(
    oi_db_query "
      SELECT CASE
        WHEN to_regclass('public.highway_edges') IS NOT NULL
         AND to_regclass('public.corridors') IS NOT NULL
         AND to_regclass('public.corridor_exits') IS NOT NULL
         AND to_regclass('public.reference_routes') IS NOT NULL
        THEN 1 ELSE 0
      END;
    " 2>/dev/null || true
  )"
  [[ "$exists" == "1" ]]
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
  local import_pbf pbf_basename pbf_stem filtered_output import_mode requested_mode signature_mode
  local flatnodes_path drop_middle mapping_file
  local import_state_file import_signature import_size_bytes stored_signature
  local legacy_create_signature="" legacy_append_signature=""
  local use_flatnodes=false flatnodes_mode threshold_bytes
  local cache_mb
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

  requested_mode="${OSM2PGSQL_MODE:-auto}"
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
  flatnodes_mode="${OI_FLATNODES_MODE:-auto}"
  cache_mb="${OI_IMPORT_CACHE_MB:-2048}"
  import_size_bytes="$(oi_file_size_bytes "$import_pbf")"
  threshold_bytes=$(( ${OI_FLATNODES_AUTO_MAX_PBF_MB:-1024} * 1024 * 1024 ))
  import_state_file="$(oi_state_file import "$OI_DATA_ROOT|$OI_DB_NAME")"
  signature_mode="$import_mode"
  if [[ "$requested_mode" == "auto" ]]; then
    signature_mode="auto"
  fi
  import_signature="$(
    {
      oi_file_signature "$import_pbf"
      printf 'mapping=%s\n' "$(oi_hash_files "$mapping_file")"
      printf 'mode=%s\n' "$signature_mode"
      printf 'drop_middle=%s\n' "$drop_middle"
      printf 'flatnodes_mode=%s\n' "$flatnodes_mode"
      printf 'cache_mb=%s\n' "$cache_mb"
    } | oi_hash_stdin
  )"
  stored_signature="$(oi_state_read "$import_state_file" signature 2>/dev/null || true)"
  if [[ "$requested_mode" == "auto" ]]; then
    legacy_create_signature="$(
      {
        oi_file_signature "$import_pbf"
        printf 'mapping=%s\n' "$(oi_hash_files "$mapping_file")"
        printf 'mode=create\n'
        printf 'drop_middle=%s\n' "$drop_middle"
        printf 'flatnodes_mode=%s\n' "$flatnodes_mode"
        printf 'cache_mb=%s\n' "$cache_mb"
      } | oi_hash_stdin
    )"
    legacy_append_signature="$(
      {
        oi_file_signature "$import_pbf"
        printf 'mapping=%s\n' "$(oi_hash_files "$mapping_file")"
        printf 'mode=append\n'
        printf 'drop_middle=%s\n' "$drop_middle"
        printf 'flatnodes_mode=%s\n' "$flatnodes_mode"
        printf 'cache_mb=%s\n' "$cache_mb"
      } | oi_hash_stdin
    )"
  fi

  case "$flatnodes_mode" in
    always)
      use_flatnodes=true
      ;;
    never)
      use_flatnodes=false
      ;;
    auto)
      if (( import_size_bytes > threshold_bytes )); then
        use_flatnodes=true
      fi
      ;;
    *)
      oi_die "invalid OI_FLATNODES_MODE=$flatnodes_mode (expected auto|always|never)"
      ;;
  esac

  if [[ "$use_flatnodes" != true ]]; then
    oi_cleanup_unused_flatnodes "$flatnodes_path"
  fi

  if oi_canonical_tables_exist && {
    [[ "$stored_signature" == "$import_signature" ]] || {
      [[ "$requested_mode" == "auto" ]] && {
        [[ "$stored_signature" == "$legacy_create_signature" ]] || [[ "$stored_signature" == "$legacy_append_signature" ]]
      }
    }
  }; then
    oi_assert_canonical_import_ready
    oi_log "Skipping canonical osm2pgsql import; input and mapping are unchanged"
    echo "  input: $import_pbf" >&2
    oi_state_write "$import_state_file" \
      signature "$import_signature" \
      source_pbf "$source_pbf" \
      import_pbf "$import_pbf" \
      mode "$import_mode" \
      requested_mode "$requested_mode" \
      use_flatnodes "$use_flatnodes" \
      completed_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf '%s\n' "$import_pbf"
    return 0
  fi

  osm2pgsql_args=(
    --slim
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

  if [[ "$use_flatnodes" == true ]]; then
    osm2pgsql_args+=(--cache=0 --flat-nodes="$(oi_container_path "$flatnodes_path")")
  else
    osm2pgsql_args+=(--cache="$cache_mb")
  fi

  oi_log "Running canonical osm2pgsql import"
  echo "  mode: $import_mode" >&2
  echo "  input: $import_pbf" >&2
  echo "  mapping: $mapping_file" >&2
  if [[ "$use_flatnodes" == true ]]; then
    echo "  flatnodes: $flatnodes_path" >&2
  else
    echo "  flatnodes: disabled (cache=${cache_mb}MB)" >&2
  fi

  oi_runner env PGPASSWORD="$OI_DB_PASSWORD" \
    osm2pgsql "${osm2pgsql_args[@]}" "$(oi_container_path "$import_pbf")" >&2 || return $?
  oi_assert_canonical_import_ready
  oi_state_write "$import_state_file" \
    signature "$import_signature" \
    source_pbf "$source_pbf" \
    import_pbf "$import_pbf" \
    mode "$import_mode" \
    requested_mode "$requested_mode" \
    use_flatnodes "$use_flatnodes" \
    completed_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

  printf '%s\n' "$import_pbf"
}

oi_extract_interstate_relation_cache() {
  local import_state_file source_pbf extractor_script cache_path cache_state_file
  local cache_signature stored_signature

  import_state_file="$(oi_state_file import "$OI_DATA_ROOT|$OI_DB_NAME")"
  source_pbf="$(oi_state_read "$import_state_file" source_pbf 2>/dev/null || true)"
  if [[ -z "$source_pbf" || ! -f "$source_pbf" ]]; then
    if [[ -f "$OI_SOURCE_CACHE_DIR/us-latest.osm.pbf" ]]; then
      source_pbf="$OI_SOURCE_CACHE_DIR/us-latest.osm.pbf"
    elif [[ -f "$OI_DOWNLOAD_DIR/us-latest.osm.pbf" ]]; then
      source_pbf="$OI_DOWNLOAD_DIR/us-latest.osm.pbf"
    else
      oi_die "cannot extract Interstate relation cache: source PBF is unavailable"
    fi
  fi

  extractor_script="$REPO_ROOT/tooling/extract_interstate_relations.py"
  cache_path="$OI_CACHE_DIR/interstate-relations.tsv"
  cache_state_file="$(oi_state_file interstate-relations "$OI_DATA_ROOT|$OI_DB_NAME")"
  cache_signature="$(
    {
      oi_file_signature "$source_pbf"
      printf 'extractor=%s\n' "$(oi_hash_files "$extractor_script")"
    } | oi_hash_stdin
  )"
  stored_signature="$(oi_state_read "$cache_state_file" signature 2>/dev/null || true)"

  if [[ -f "$cache_path" && "$stored_signature" == "$cache_signature" ]]; then
    printf '%s\n' "$cache_path"
    return 0
  fi

  oi_log "Extracting Interstate relation cache from source PBF"
  python3 "$extractor_script" \
    --source-pbf "$source_pbf" \
    --output "$cache_path" || return $?

  oi_state_write "$cache_state_file" \
    signature "$cache_signature" \
    source_pbf "$source_pbf" \
    cache_path "$cache_path" \
    completed_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

  printf '%s\n' "$cache_path"
}

oi_apply_derive() {
  local derive_file="$REPO_ROOT/schema/derive.sql"
  local derive_state_file import_state_file import_signature derive_signature derive_sql_signature derive_code_signature
  local reachability_signature relation_cache_file relation_cache_signature
  local -a derive_source_files=()

  oi_guard_no_reachability_clears "$derive_file"

  import_state_file="$(oi_state_file import "$OI_DATA_ROOT|$OI_DB_NAME")"
  import_signature="$(oi_state_read "$import_state_file" signature 2>/dev/null || true)"
  [[ -n "$import_signature" ]] || import_signature="no-import-state"

  derive_state_file="$(oi_state_file derive "$OI_DATA_ROOT|$OI_DB_NAME")"
  derive_source_files=(
    "$REPO_ROOT/Cargo.toml"
    "$REPO_ROOT/Cargo.lock"
    "$REPO_ROOT/crates/core/Cargo.toml"
    "$REPO_ROOT/crates/derive/Cargo.toml"
  )
  while IFS= read -r rel_path; do
    derive_source_files+=("$REPO_ROOT/$rel_path")
  done < <(
    cd "$REPO_ROOT" && find crates/core/src crates/derive/src -type f -name '*.rs' | LC_ALL=C sort
  )
  derive_sql_signature="$(oi_hash_files "$derive_file")"
  derive_code_signature="$(oi_hash_files "${derive_source_files[@]}")"
  relation_cache_file="$(oi_extract_interstate_relation_cache)"
  [[ -f "$relation_cache_file" ]] || oi_die "missing Interstate relation cache: $relation_cache_file"
  relation_cache_signature="$(oi_file_signature "$relation_cache_file")"
  reachability_signature="$(
    {
      oi_db_query "SELECT COUNT(*), COALESCE(MAX(updated_at)::text, '') FROM exit_poi_reachability;" 2>/dev/null || true
      oi_db_query "SELECT COUNT(*), COALESCE(MAX(updated_at)::text, '') FROM osrm_snap_hints;" 2>/dev/null || true
    } | oi_hash_stdin
  )"
  derive_signature="$(
    {
      printf 'import=%s\n' "$import_signature"
      printf 'derive_sql=%s\n' "$derive_sql_signature"
      printf 'derive_code=%s\n' "$derive_code_signature"
      printf 'relation_cache=%s\n' "$relation_cache_signature"
      printf 'reachability=%s\n' "$reachability_signature"
    } | oi_hash_stdin
  )"

  if oi_derive_outputs_ready && [[ "$(oi_state_read "$derive_state_file" signature 2>/dev/null || true)" == "$derive_signature" ]]; then
    oi_log "Skipping derive; SQL and Rust builders are unchanged"
    return 0
  fi

  oi_log "Applying deterministic SQL projection"
  oi_db_exec psql -U "$OI_DB_USER" -d "$OI_DB_NAME" -v ON_ERROR_STOP=1 < "$derive_file"

  oi_log "Building graph, corridors, and reference routes"
  oi_runner cargo run --release -p openinterstate-derive -- \
    --database-url "$PRODUCT_DB_URL" \
    --interstate-relation-cache "$(oi_container_path "$relation_cache_file")" \
    all
  oi_state_write "$derive_state_file" \
    signature "$derive_signature" \
    import_signature "$import_signature" \
    completed_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
}

oi_export_release() {
  local release_id="$1"
  local source_pbf="$2"
  local import_pbf="${3:-$2}"
  local source_url="${4:-}"
  local output_root="${5:-$OI_RELEASE_DIR}"
  local output_dir archive_path
  local release_state_file export_signature derive_state_file derive_signature exporter_signature
  local -a export_args

  source_pbf="$(oi_stage_input_file "$source_pbf")"
  import_pbf="$(oi_stage_input_file "$import_pbf")"
  output_root="$(oi_managed_path "$output_root")"
  output_dir="$output_root/$release_id"
  archive_path="$output_root/openinterstate-$release_id.tar.gz"
  release_state_file="$(oi_state_file release "$output_dir")"
  derive_state_file="$(oi_state_file derive "$OI_DATA_ROOT|$OI_DB_NAME")"
  derive_signature="$(oi_state_read "$derive_state_file" signature 2>/dev/null || true)"
  exporter_signature="$(oi_hash_files "$REPO_ROOT/tooling/export_release.py")"
  export_signature="$(
    {
      printf 'release_id=%s\n' "$release_id"
      printf 'source=%s\n' "$(oi_file_signature "$source_pbf")"
      printf 'import=%s\n' "$(oi_file_signature "$import_pbf")"
      printf 'source_url=%s\n' "$source_url"
      printf 'derive=%s\n' "$derive_signature"
      printf 'exporter=%s\n' "$exporter_signature"
    } | oi_hash_stdin
  )"

  mkdir -p "$output_root"

  if [[ "$(oi_state_read "$release_state_file" signature 2>/dev/null || true)" == "$export_signature" ]] \
    && [[ -d "$output_dir" && -f "$archive_path" && -f "$output_dir/manifest.json" && -f "$output_dir/checksums.txt" && -f "$output_dir/source_lineage.json" ]]; then
    oi_log "Skipping release export; artifacts are current"
    echo "  release id: $release_id" >&2
    echo "  output dir: $output_dir" >&2
    return 0
  fi

  oi_log "Exporting release artifacts"
  echo "  release id: $release_id" >&2
  echo "  output dir: $output_dir" >&2

  export_args=(
    python3
    /workspace/tooling/export_release.py
    --database-url "$PRODUCT_DB_URL"
    --release-id "$release_id"
    --output-dir "$(oi_container_path "$output_dir")"
    --state-dir "$(oi_container_path "$OI_STATE_DIR")"
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
  oi_state_write "$release_state_file" \
    signature "$export_signature" \
    release_id "$release_id" \
    output_dir "$output_dir" \
    archive_path "$archive_path" \
    completed_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
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
