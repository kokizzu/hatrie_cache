#!/usr/bin/env bash
set -euo pipefail

temporary_files=(
  hat/hatSql/round27_compat.go
  scripts/inspect-next-candidate-round27.sh
  scripts/inspect-quantile-round27.sh
  scripts/inspect-typed-table-mutation-round27.sh
  scripts/inspect-t-u14-docs-round27.sh
)
for file in "${temporary_files[@]}"; do
  if [[ -e "$file" ]]; then
    printf 'temporary file remains: %s\n' "$file" >&2
    exit 1
  fi
done

git diff --check
git status --short --untracked-files=all
