#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_BACKLOG.md \
	BENCHMARK.md \
	TYPED_TABLE_SPARSE_MARK_CACHE.md \
	hat/hatSql/typed_table.go \
	hat/hatSql/typed_table_sparse_mark_cache.go \
	hat/hatSql/ch006_sparse_mark_cache_test.go \
	hat/hatSql/ch006_sparse_mark_cache_baseline_benchmark_test.go \
	hat/hatSql/ch006_sparse_mark_cache_benchmark_test.go \
	scripts/test-ch006.sh \
	scripts/commit-ch006.sh \
	scripts/push-ch006.sh \
	scripts/status-ch006.sh
git commit -m "feat: add sparse primary mark cache"
