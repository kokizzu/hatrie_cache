#!/usr/bin/env bash
set -euo pipefail

forbidden=(
  hat/hatSql/round26_compat.go
  scripts/inspect-chg01.sh
  scripts/inspect-source-health-call.sh
  scripts/inspect-source-health-implementation.sh
  scripts/inspect-typed-table-types.sh
  scripts/inspect-mg07-docs.sh
  scripts/inspect-mg07-doc-tail.sh
)
for path in "${forbidden[@]}"; do
  if [[ -e "$path" ]]; then
    printf 'temporary file remains: %s\n' "$path" >&2
    exit 1
  fi
done

git diff --check
git status --short
git diff --stat
