#!/usr/bin/env bash
set -euo pipefail

git diff --cached --quiet && {
  printf '%s\n' 'No staged T202 changes.'
  exit 1
}
git commit -m 'feat: add automatic replica leader election'
