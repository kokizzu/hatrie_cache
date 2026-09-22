#!/usr/bin/env bash
set -euo pipefail

git status --short
printf '%s\n' '--- staged paths ---'
git diff --cached --name-status
