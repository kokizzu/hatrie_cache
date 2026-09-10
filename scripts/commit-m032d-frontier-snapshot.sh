#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION.md \
	Makefile \
	README.md \
	SQL_FRONTIER_SNAPSHOTS.md \
	SQL_SNAPSHOT_PROVIDER.md \
	SQL_SOURCE_FRONTIERS.md \
	hat/hatSql/sql_frontier_snapshot_benchmark_test.go \
	hat/hatSql/sql_frontier_snapshot_provider.go \
	hat/hatSql/sql_frontier_snapshot_provider_test.go \
	scripts/benchmark-m032d-frontier-snapshot.sh \
	scripts/commit-m032d-frontier-snapshot.sh \
	scripts/format-m032d-frontier-snapshot.sh \
	scripts/push-m032d-frontier-snapshot.sh \
	scripts/review-m032d-frontier-snapshot.sh \
	scripts/test-m032d-frontier-snapshot.sh \
	scripts/test-race-m032d-frontier-snapshot.sh
git diff --cached --check
git commit -m "feat(sql): add frontier-bound snapshots"
