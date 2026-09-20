#!/usr/bin/env bash
set -euo pipefail

git diff --cached --quiet && {
  printf '%s\n' 'no staged M-U34 changes to commit' >&2
  exit 1
}
git commit -m 'feat(changefeed): add historical subscription checkpoints'
