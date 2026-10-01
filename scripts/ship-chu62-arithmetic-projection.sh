#!/usr/bin/env bash
set -euo pipefail

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CHU62_ARITHMETIC_PROJECTION.md \
	SQL_IMPROVEMENTS_100.md \
	Makefile \
	hat/hatSql/columnar_arithmetic_projection.go \
	hat/hatSql/chu62_arithmetic_projection_benchmark_test.go \
	hat/hatSql/chu62_arithmetic_projection_test.go \
	hat/hatSql/query.go
git add -- \
	scripts/format-chu62-arithmetic-projection.sh \
	scripts/test-chu62-arithmetic-projection.sh \
	scripts/benchmark-chu62-arithmetic-projection.sh \
	scripts/race-chu62-arithmetic-projection.sh \
	scripts/vet-chu62-arithmetic-projection.sh \
	scripts/test-chu62-sql-package.sh \
	scripts/test-chu62-all.sh \
	scripts/review-chu62-arithmetic-projection.sh \
	scripts/ship-chu62-arithmetic-projection.sh
git diff --cached --check
git commit -m 'feat(sql): vectorize arithmetic projections [skip ci]'
git push origin HEAD
