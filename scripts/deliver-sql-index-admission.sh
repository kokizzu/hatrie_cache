#!/bin/sh
set -eu

git add Makefile README.md BENCHMARK.md \
	hat/hatCache/main.go \
	hat/hatCache/sql_query.go \
	hat/hatCache/sql_index_admission_test.go \
	hat/hatCache/sql_index_admission_benchmark_test.go \
	scripts/bench-sql-index-admission.sh \
	scripts/deliver-sql-index-admission.sh \
	scripts/format-sql-index-admission.sh \
	scripts/inspect-sql-index-admission.sh \
	scripts/test-sql-index-admission.sh
git diff --cached --check
git commit -m 'feat(sql): bound automatic JSON index builds'
git push origin master
