#!/usr/bin/env bash
set -euo pipefail

	gofmt -w \
	hat/hatSql/m210_retained_sql_snapshot.go \
	hat/hatSql/m211_frontier_bounds_baseline_benchmark_test.go \
	hat/hatSql/m211_frontier_bounds_benchmark_test.go \
	hat/hatSql/m211_frontier_bounds_test.go \
	hat/hatSql/sql_as_of.go \
	hat/hatSql/sql_distributed_frontier.go \
	hat/hatSql/sql_frontier_bounds.go \
	hat/hatSql/sql_frontier_snapshot_provider.go \
	hat/hatSql/typed_table_mvcc.go
