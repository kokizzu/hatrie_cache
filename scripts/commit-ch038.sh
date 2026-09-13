#!/usr/bin/env bash
set -euo pipefail
git add -- Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md SQL_BITMAP_AGGREGATES.md \
	hat/hatSql/query.go hat/hatSql/bitmap_aggregates.go hat/hatSql/bitmap_aggregate_test.go \
	scripts/benchmark-ch038-after.sh scripts/benchmark-ch038-before.sh scripts/check-ch038.sh \
	scripts/commit-ch038.sh scripts/format-ch038.sh scripts/race-ch038.sh scripts/review-ch038.sh \
	scripts/test-ch038-all.sh scripts/test-ch038-repo.sh scripts/test-ch038.sh scripts/vet-ch038.sh \
	scripts/push-ch038.sh
git diff --cached --check
git commit -m "feat: add SQL bitmap aggregates"
