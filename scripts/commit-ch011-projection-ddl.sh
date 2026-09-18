#!/usr/bin/env bash
set -euo pipefail

git diff --cached --quiet && {
  printf '%s\n' 'No staged CH-011 changes.' >&2
  exit 1
}
git commit -m 'feat: add projection DDL [skip ci]'
