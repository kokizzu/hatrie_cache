#!/usr/bin/env bash
set -euo pipefail

git diff --cached --quiet && {
  printf '%s\n' 'no staged TU39 changes to commit' >&2
  exit 1
}
git commit -m 'feat(changefeed): add named space journal feeds'
