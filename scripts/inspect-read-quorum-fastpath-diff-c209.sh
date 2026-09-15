#!/usr/bin/env bash
set -euo pipefail

git status --short
printf '%s\n' '--- staged paths ---'
git diff --cached --name-only
printf '%s\n' '--- staged diff check ---'
git diff --cached --check
git diff --cached --stat
git diff --cached -- hat/hatReplication/read_quorum.go
