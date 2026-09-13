#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md SQL_TDIGEST_PERCENTILE.md \
  hat/hatDataStructure/tdigest.go hat/hatDataStructure/tdigest_test.go \
  hat/hatSql/approx_aggregate.go hat/hatSql/query.go hat/hatSql/tdigest_aggregate_test.go \
  scripts/benchmark-ch040-after.sh scripts/benchmark-ch040-before.sh scripts/check-ch040.sh \
  scripts/commit-ch040.sh scripts/format-ch040.sh scripts/push-ch040.sh scripts/race-ch040.sh \
  scripts/review-ch040.sh scripts/test-ch040-all.sh scripts/test-ch040-repo.sh scripts/test-ch040.sh scripts/vet-ch040.sh
git diff --cached --check
git diff --cached --stat
git commit -m "feat: add t-digest percentile aggregate"
