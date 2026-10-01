#!/usr/bin/env bash
set -euo pipefail

printf 'branch: '
git branch --show-current
printf '%s\n' 'status:'
git status --short
printf '%s\n' 'diff stat:'
git diff --stat
