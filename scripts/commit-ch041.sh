#!/usr/bin/env bash
set -euo pipefail
git add -- Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md SQL_BOUNDED_GROUP_ARRAY.md \
	hat/hatSql/aggregate_collection_test.go hat/hatSql/aggregate_collections.go \
	scripts/benchmark-ch041-after.sh scripts/benchmark-ch041-before.sh scripts/check-ch041.sh \
	scripts/commit-ch041.sh scripts/format-ch041.sh scripts/race-ch041.sh scripts/review-ch041.sh \
	scripts/test-ch041-all.sh scripts/test-ch041-repo.sh scripts/test-ch041.sh scripts/vet-ch041.sh \
	scripts/push-ch041.sh
git diff --cached --check
git commit -m "feat: add bounded group array aggregate"
