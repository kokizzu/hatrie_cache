#!/usr/bin/env bash
set -euo pipefail

paths=(
	README.md
	INSPIRATION_BACKLOG.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	Makefile
)

printf '%s\n' '--- feature diff stats ---'
git diff --stat -- "${paths[@]}"
printf '%s\n' '--- whitespace check ---'
git diff --check -- "${paths[@]}"
printf '%s\n' '--- feature diff ---'
git diff -- "${paths[@]}"
printf '%s\n' '--- worktree status ---'
git status --short
