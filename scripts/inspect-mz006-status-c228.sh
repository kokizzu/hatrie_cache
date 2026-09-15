#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Worktree status:'
git status --short
printf '%s\n' '' 'MZ-006 tracked diff check:'
git diff --check -- \
  MZ006_OBJECT_STORE_GARBAGE_COLLECTION.md \
  INSPIRATION_BACKLOG.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  hat/hatBackup/object_store_gc.go \
  hat/hatBackup/mz006_object_store_gc_test.go \
  hat/hatBackup/mz006_object_store_gc_benchmark_test.go
git diff --stat -- \
  MZ006_OBJECT_STORE_GARBAGE_COLLECTION.md \
  INSPIRATION_BACKLOG.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  hat/hatBackup/object_store_gc.go \
  hat/hatBackup/mz006_object_store_gc_test.go \
  hat/hatBackup/mz006_object_store_gc_benchmark_test.go
