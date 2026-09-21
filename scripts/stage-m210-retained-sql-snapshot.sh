#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	INSPIRATION_ROUND2.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	M210_RETAINED_SQL_SNAPSHOTS.md \
	hat/hatSql/m210_retained_sql_snapshot.go \
	hat/hatSql/m210_retained_sql_snapshot_test.go \
	hat/hatSql/m210_retained_sql_snapshot_baseline_benchmark_test.go \
	hat/hatSql/m210_retained_sql_snapshot_benchmark_test.go \
	scripts/format-m210-retained-sql-snapshot.sh \
	scripts/test-m210-retained-sql-snapshot.sh \
	scripts/test-m210-retained-sql-snapshot-package.sh \
	scripts/benchmark-m210-retained-sql-snapshot-baseline.sh \
	scripts/benchmark-m210-retained-sql-snapshot.sh \
	scripts/race-m210-retained-sql-snapshot.sh \
	scripts/vet-m210-retained-sql-snapshot.sh \
	scripts/verify-m210-retained-sql-snapshot.sh \
	scripts/stage-m210-retained-sql-snapshot.sh \
	scripts/commit-m210-retained-sql-snapshot.sh \
	scripts/push-m210-retained-sql-snapshot.sh
git diff --cached --check
