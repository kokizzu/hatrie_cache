#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Staged paths:'
git diff --cached --name-status
printf '%s\n' 'Staged summary:'
git diff --cached --stat
printf '%s\n' 'Staged whitespace check:'
git diff --cached --check
