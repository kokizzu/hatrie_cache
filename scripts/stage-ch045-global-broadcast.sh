#!/usr/bin/env bash
set -euo pipefail

git add Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH045_GLOBAL_JOIN_BROADCAST_PLANNING.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  hat/hatSql/ch045_global_join_broadcast_plan.go \
  hat/hatSql/ch045_global_join_broadcast_plan_test.go \
  hat/hatSql/ch045_global_join_broadcast_plan_benchmark_test.go \
  scripts/benchmark-ch045-global-broadcast.sh \
  scripts/commit-ch045-global-broadcast.sh \
  scripts/format-ch045-global-broadcast-benchmark.sh \
  scripts/format-ch045-global-broadcast.sh \
  scripts/push-ch045-global-broadcast.sh \
  scripts/race-ch045-global-broadcast.sh \
  scripts/stage-ch045-global-broadcast.sh \
  scripts/test-ch045-global-broadcast-package.sh \
  scripts/test-ch045-global-broadcast.sh \
  scripts/verify-ch045-global-broadcast-docs.sh \
  scripts/vet-ch045-global-broadcast.sh

git diff --cached --check
git status --short
