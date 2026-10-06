#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
printf '%s\n' '--- staged paths ---'
git diff --cached --name-only
printf '%s\n' '--- staged stat ---'
git diff --cached --stat
