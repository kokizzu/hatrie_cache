#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' 'T115 staged files:'
git diff --cached --name-status
printf '%s\n' 'T115 staged summary:'
git diff --cached --stat
git diff --cached --check
