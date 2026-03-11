#!/usr/bin/env bash

# Shared helpers for OpenInterstate pipeline scripts.

PIPELINE_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="$(cd "$PIPELINE_LIB_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PIPELINE_DIR/.." && pwd)"
DEFAULT_ENV_FILE="$REPO_ROOT/.env.pipeline"

oi_log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
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

oi_compose_cmd() {
  local env_file="${1:-$DEFAULT_ENV_FILE}"
  OI_COMPOSE_CMD=(docker compose)
  if [[ -f "$env_file" ]]; then
    OI_COMPOSE_CMD+=(--env-file "$env_file")
  fi
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

oi_ensure_python_env() {
  local venv_dir="$REPO_ROOT/.venv"

  if [[ ! -x "$venv_dir/bin/python" ]]; then
    oi_require_cmd python3
    python3 -m venv "$venv_dir"
  fi

  OI_VENV_PYTHON="$venv_dir/bin/python"
  "$OI_VENV_PYTHON" -m pip install --quiet -r "$REPO_ROOT/requirements-release.txt"
}
