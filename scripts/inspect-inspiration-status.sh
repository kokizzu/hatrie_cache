#!/usr/bin/env bash
set -euo pipefail

printf 'Branch:\n'
git branch --show-current
printf '\nWorktree status:\n'
git status --short --branch
printf '\nRecent inspiration commits:\n'
git log --oneline --decorate -20
printf '\nIdea catalog rows:\n'
rg -n 'T-U[0-9]+' PRODUCT_IDEA_GAPS.md
printf '\nTemporary T-U05 targets (should be empty after cleanup):\n'
rg -n 'tu05|TU05' Makefile scripts || true
