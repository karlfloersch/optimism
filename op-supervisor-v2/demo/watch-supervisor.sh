#!/usr/bin/env bash

set -u

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
vars_file="${script_dir}/variables.sh"

if [[ ! -f "${vars_file}" ]]; then
  echo "variables.sh not found at ${vars_file}" >&2
  exit 1
fi

# shellcheck source=/dev/null
source "${vars_file}"

search_string="${1:-}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found on PATH" >&2
  exit 1
fi

if [[ -z "${SV2_CONTAINER_NAME:-}" ]]; then
  echo "SV2_CONTAINER_NAME is empty after sourcing variables. Is the supervisor running?" >&2
  exit 1
fi

# If multiple names are present, select the first line
resolved_container="$(echo "${SV2_CONTAINER_NAME}" | head -n1)"

if ! docker ps --format '{{.Names}}' | grep -q "^${resolved_container}$"; then
  echo "Container '${resolved_container}' not found among running containers" >&2
  docker ps --format '{{.Names}}\t{{.Image}}' | grep -i supervisor || true
  exit 1
fi

echo "Watching logs for container: ${resolved_container}" >&2
if [[ -n "${search_string}" ]]; then
  echo "Filtering for: ${search_string}" >&2
  # --line-buffered keeps output responsive
  docker logs -f "${resolved_container}" 2>&1 | grep --line-buffered -F "${search_string}"
else
  docker logs -f "${resolved_container}"
fi


