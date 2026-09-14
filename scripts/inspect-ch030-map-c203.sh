#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '== CH030 Makefile block =='
rg -n -A45 -B5 'ch030-map-c203' Makefile || true
printf '%s\n' '== Makefile diff summary =='
git diff --stat -- Makefile
printf '%s\n' '== Worktree summary =='
git status --short
