#!/usr/bin/env bash
set -euo pipefail

expected=(
  Makefile
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  scripts/stage-ledger-corrections.sh
  scripts/commit-ledger-corrections.sh
  scripts/push-ledger-corrections.sh
)
declare -A allowed=()
for path in "${expected[@]}"; do
  allowed["$path"]=1
done

mapfile -t actual < <(git diff --cached --name-only --diff-filter=ACMRT)
if (( ${#actual[@]} != ${#expected[@]} )); then
  printf 'unexpected staged file count: got %d, want %d\n' "${#actual[@]}" "${#expected[@]}" >&2
  printf '%s\n' "${actual[@]}" >&2
  exit 1
fi
for path in "${actual[@]}"; do
  if [[ -z "${allowed[$path]+present}" ]]; then
    printf 'unexpected staged path: %s\n' "$path" >&2
    exit 1
  fi
  unset 'allowed[$path]'
done
if (( ${#allowed[@]} != 0 )); then
  printf 'missing staged paths:\n' >&2
  printf '%s\n' "${!allowed[@]}" >&2
  exit 1
fi

git diff --cached --check
git commit -m 'docs: sync adopted idea ledger [skip ci]'
