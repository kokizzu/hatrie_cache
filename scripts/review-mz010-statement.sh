#!/usr/bin/env bash
set -euo pipefail
git status --short
git diff --check
git diff --stat -- \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	MZ010_SQL_SUBSCRIPTIONS.md \
	Makefile \
	hat/hatSql/mz010_sql_subscription_statement.go \
	hat/hatSql/mz010_sql_subscription_statement_test.go \
	hat/hatSql/mz010_sql_subscription_statement_benchmark_test.go \
	scripts/benchmark-mz010-statement.sh \
	scripts/format-mz010-statement.sh \
	scripts/race-mz010-statement.sh \
	scripts/review-mz010-statement.sh \
	scripts/test-mz010-statement-package.sh \
	scripts/test-mz010-statement.sh \
	scripts/vet-mz010-statement.sh
