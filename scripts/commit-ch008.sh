#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	C208_RESULT_CACHE_METRICS.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	hat/hatSql/c208_query_cache_metrics_test.go \
	hat/hatSql/c208_result_cache_key_benchmark_test.go \
	hat/hatSql/query.go \
	hat/hatSql/sql_result_cache.go \
	scripts/benchmark-ch008-baseline.sh \
	scripts/benchmark-ch008-baseline_test.txt \
	scripts/benchmark-ch008.sh \
	scripts/check-ch008.sh \
	scripts/commit-ch008.sh \
	scripts/format-ch008.sh \
	scripts/push-ch008.sh \
	scripts/race-ch008.sh \
	scripts/test-ch008-package.sh \
	scripts/test-ch008-red.sh \
	scripts/vet-ch008.sh
git diff --cached --check
git commit -m "feat: add settings-aware result-cache keys"
