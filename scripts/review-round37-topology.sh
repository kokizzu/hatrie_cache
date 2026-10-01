#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
printf '%s\n' 'status:'
git status --short
printf '%s\n' 'diff stat:'
git diff --cached --stat
printf '%s\n' 'changed paths:'
git status --short
