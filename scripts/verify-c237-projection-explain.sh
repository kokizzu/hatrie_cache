#!/usr/bin/env bash
set -euo pipefail

for path in \
  hat/hatSql/codex_compile_shim.go \
  scripts/codex-inspect-c237.sh \
  scripts/codex-inspect-c237-targets.sh \
  scripts/codex-inspect-c237-tests.sh; do
  if [[ -e "$path" ]]; then
    printf 'temporary file remains: %s\n' "$path" >&2
    exit 1
  fi
done

git diff --check
git status --short
git diff --stat
