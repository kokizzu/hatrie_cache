#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Worktree status:'
git status --short
printf '%s\n' '' 'MZ-003 tracked diff check:'
git diff --check -- \
  MZ003_FRONTIER_COMPACTION_SCHEDULER.md \
  INSPIRATION_BACKLOG.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  hat/hatPipeline/frontier_compaction_scheduler.go \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/mz003_frontier_compaction_scheduler_test.go
git diff --stat -- \
  MZ003_FRONTIER_COMPACTION_SCHEDULER.md \
  INSPIRATION_BACKLOG.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  hat/hatPipeline/frontier_compaction_scheduler.go \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/mz003_frontier_compaction_scheduler_test.go
