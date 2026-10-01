#!/usr/bin/env bash
set -euo pipefail

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CHU63_CASE_PROJECTION.md \
	SQL_IMPROVEMENTS_100.md \
	Makefile \
	hat/hatSql/columnar_case_projection.go \
	hat/hatSql/chu63_case_projection_benchmark_test.go \
	hat/hatSql/chu63_case_projection_test.go \
	hat/hatSql/query.go
git add -- \
	scripts/format-chu63-case-projection.sh \
	scripts/test-chu63-case-projection.sh \
	scripts/benchmark-chu63-case-projection.sh \
	scripts/race-chu63-case-projection.sh \
	scripts/vet-chu63-case-projection.sh \
	scripts/test-chu63-sql-package.sh \
	scripts/review-chu63-case-projection.sh \
	scripts/ship-chu63-case-projection.sh
git diff --cached --check
git commit -m 'feat(sql): vectorize CASE projections [skip ci]'
git push origin HEAD
