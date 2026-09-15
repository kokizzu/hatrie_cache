#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
printf '%s\n' '=== worktree status ==='
git status --short
printf '%s\n' '=== unstaged stat ==='
git diff --stat
printf '%s\n' '=== staged stat ==='
git diff --cached --stat
