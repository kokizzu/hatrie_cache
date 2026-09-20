#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git diff --cached --quiet && {
  printf '%s\n' 'No staged TT051 changes.' >&2
  exit 1
}
git commit -m 'feat(journal): add partitioned synchronous durability'
