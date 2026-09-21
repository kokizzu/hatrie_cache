#!/usr/bin/env bash
set -euo pipefail

git diff --cached --quiet && {
  printf '%s\n' 'C240 commit aborted: no staged changes.' >&2
  exit 1
}
git commit -m "feat: add read-only backup attachments"
