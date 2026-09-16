#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
printf '%s\n' '--- staged paths ---'
git diff --cached --name-status
printf '%s\n' '--- staged stat ---'
git diff --cached --stat
printf '%s\n' '--- staged Makefile target block ---'
git show :Makefile | tail -n 45
