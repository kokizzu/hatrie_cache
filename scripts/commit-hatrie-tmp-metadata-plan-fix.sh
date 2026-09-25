#!/usr/bin/env bash
set -euo pipefail

expected_paths=(
  Makefile
  scripts/audit-hatrie-tmp.sh
  scripts/stage-hatrie-tmp-metadata-plan-fix.sh
  scripts/commit-hatrie-tmp-metadata-plan-fix.sh
  scripts/push-hatrie-tmp-metadata-plan-fix.sh
)

mapfile -t staged_paths < <(git diff --cached --name-only --)
if (( ${#staged_paths[@]} != ${#expected_paths[@]} )); then
  printf 'unexpected staged path count: expected %s, got %s\n' "${#expected_paths[@]}" "${#staged_paths[@]}" >&2
  printf 'Staged paths:\n' >&2
  printf '  %s\n' "${staged_paths[@]}" >&2
  exit 1
fi
for expected in "${expected_paths[@]}"; do
  found=0
  for staged in "${staged_paths[@]}"; do
    [[ "$staged" == "$expected" ]] && found=1
  done
  (( found == 1 )) || {
    printf 'expected staged path is missing: %s\n' "$expected" >&2
    exit 1
  }
done

git diff --cached --check
git commit -m 'chore: remove tmp cleanup metadata residue [skip ci]'
