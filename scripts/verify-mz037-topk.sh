#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' '--- status ---'
git status --short --branch
printf '%s\n' '--- diff stat ---'
git diff --stat
printf '%s\n' '--- changed paths ---'
git diff --name-only
