#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md SQL_AUTO_COUNT_DISTINCT.md \
  hat/hatSql/query.go hat/hatSql/approx_aggregate.go hat/hatSql/auto_distinct.go hat/hatSql/auto_distinct_test.go \
  scripts/benchmark-ch039-after.sh scripts/benchmark-ch039-before.sh scripts/check-ch039.sh \
  scripts/commit-ch039.sh scripts/format-ch039.sh scripts/push-ch039.sh scripts/race-ch039.sh scripts/review-ch039.sh \
  scripts/test-ch039-all.sh scripts/test-ch039-repo.sh scripts/test-ch039.sh scripts/vet-ch039.sh
git diff --cached --check
git diff --cached --stat
git commit -m "feat: add automatic distinct counting"
