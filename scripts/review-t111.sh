#!/usr/bin/env bash
set -euo pipefail

paths=(
  README.md
  BENCHMARK.md
  INSPIRATION.md
  cmd/hatrie-cache/main.go
  cmd/hatrie-cache/main_test.go
  hat/hatCache/leveldb_store.go
  hat/hatCache/pebble_generation.go
  hat/hatCache/pebble_store.go
  hat/hatCache/storage_size_limit.go
  hat/hatCache/storage_size_limit_test.go
  persistent_storage_size_limit_api.go
  scripts/test-t111.sh
  scripts/format-t111.sh
  scripts/benchmark-t111.sh
  scripts/test-race-t111.sh
  scripts/vet-t111.sh
  scripts/review-t111.sh
  scripts/stage-t111.sh
  scripts/commit-t111.sh
  scripts/push-t111.sh
)

git status --short
printf '%s\n' '--- T111 diff stat ---'
git diff --stat -- "${paths[@]}"
printf '%s\n' '--- T111 diff check ---'
git diff --check -- "${paths[@]}"
printf '%s\n' '--- LevelDB diff ---'
git diff --unified=2 -- hat/hatCache/leveldb_store.go
printf '%s\n' '--- T111 Makefile targets ---'
git diff -- Makefile | rg -n '^[+].*(t111|T111)|^[-].*(audit-inspiration-next|inspect-t111)' || true
printf '%s\n' '--- staged T111 paths ---'
git diff --cached --name-status -- Makefile "${paths[@]}"
git diff --cached --stat -- Makefile "${paths[@]}"
git diff --cached --check -- Makefile "${paths[@]}"
