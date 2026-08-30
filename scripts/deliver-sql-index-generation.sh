#!/bin/sh
set -eu

git add Makefile README.md BENCHMARK.md \
	hat/hatCache/main.go \
	hat/hatCache/sql_query.go \
	hat/hatCache/sql_index_snapshot_test.go \
	hat/hatCache/sql_index_generation_benchmark_test.go \
	scripts/bench-sql-index-generation.sh \
	scripts/deliver-sql-index-generation.sh \
	scripts/format-sql-index-generation.sh \
	scripts/inspect-sql-index-freshness.sh \
	scripts/inspect-sql-index-generation-docs.sh
git diff --cached --check
git commit -m 'perf(sql): track indexed source write generations'
git push origin master
