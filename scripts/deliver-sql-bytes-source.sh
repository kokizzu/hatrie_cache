#!/bin/sh
set -eu

git add Makefile README.md BENCHMARK.md \
	hat/hatCache/sql_query.go \
	hat/hatCache/sql_bytes_source_test.go \
	hat/hatCache/sql_bytes_source_benchmark_test.go \
	scripts/bench-sql-bytes-source.sh \
	scripts/deliver-sql-bytes-source.sh \
	scripts/format-sql-bytes-source.sh \
	scripts/inspect-sql-bytes-source.sh \
	scripts/test-sql-bytes-source.sh
git diff --cached --check
git commit -m 'perf(sql): borrow in-memory byte JSON sources'
git push origin master
