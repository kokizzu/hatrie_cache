#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
printf '%s\n' '--- staged T062 paths ---'
git diff --cached --name-status
git status --short
