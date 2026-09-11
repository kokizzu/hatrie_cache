#!/usr/bin/env bash
set -euo pipefail

for prefix in CH MZ TT; do
  count=$(rg -c "^\\| ${prefix}-[0-9]{3} \\|" ENGINE_IDEAS.md || true)
  printf '%s candidates: %s\n' "$prefix" "$count"
  test "$count" -eq 50
done
