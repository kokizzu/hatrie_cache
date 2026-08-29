#!/bin/sh
set -eu

git add -- Makefile hat/hatSql/query.go hat/hatCache/sql_columnar_mixed_conjunction_test.go hat/hatCache/sql_columnar_mixed_conjunction_benchmark_test.go scripts/test-sql-columnar-mixed-conjunction.sh scripts/format-sql-columnar-mixed-conjunction.sh scripts/benchmark-sql-columnar-mixed-conjunction.sh scripts/review-sql-columnar-mixed-conjunction.sh scripts/commit-sql-columnar-mixed-conjunction.sh
git diff --cached --check
git commit -m "perf: fuse mixed SQL columnar conjunctions"
git push origin master
