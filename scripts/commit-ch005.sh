#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_BACKLOG.md \
	BENCHMARK.md \
	TYPED_TABLE_STORAGE_EVENTS.md \
	hat/hatSql/typed_table.go \
	hat/hatSql/typed_table_patch_parts.go \
	hat/hatSql/typed_table_storage_events.go \
	hat/hatSql/ch005_storage_events_test.go \
	hat/hatSql/ch005_storage_events_baseline_benchmark_test.go \
	hat/hatSql/ch005_storage_events_benchmark_test.go \
	scripts/test-ch005.sh \
	scripts/commit-ch005.sh \
	scripts/push-ch005.sh \
	scripts/status-ch005.sh
git commit -m "feat: add typed-table storage event log"
