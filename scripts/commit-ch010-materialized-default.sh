#!/usr/bin/env bash
set -euo pipefail

git diff --cached --quiet && {
  printf '%s\n' 'No staged CH-010 changes.' >&2
  exit 1
}
git commit -m 'feat: add materialized and default columns [skip ci]'
