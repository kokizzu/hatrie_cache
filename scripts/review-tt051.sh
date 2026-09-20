#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'TT051 worktree status:'
git status --short
printf '%s\n' 'TT051 diff stat:'
git diff --stat
printf '%s\n' 'TT051 unstaged diff check:'
git diff --check
printf '%s\n' 'TT051 changed paths:'
git status --short | awk '{print $2}' | sort -u
printf '%s\n' 'TT051 Makefile diff:'
git diff -- Makefile
