#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Working-tree status:'
git status --short
printf '%s\n' 'Unstaged diff stat:'
git diff --stat
printf '%s\n' 'Staged diff stat:'
git diff --cached --stat
printf '%s\n' 'Unstaged diff check:'
git diff --check
printf '%s\n' 'Staged diff check:'
git diff --cached --check
printf '%s\n' 'Unstaged changed paths:'
git diff --name-only
printf '%s\n' 'Staged changed paths:'
git diff --cached --name-only
