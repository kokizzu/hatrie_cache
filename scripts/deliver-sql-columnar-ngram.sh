#!/bin/sh
set -eu

paths='COLUMNAR_NGRAMS.md Makefile README.md hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_ngram_test.go hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/columnar_ngram_benchmark_test.go hat/hatSql/columnar_ngram_segment_test.go scripts/audit-sql-columnar-ngram-security.sh scripts/benchmark-sql-columnar-ngram.sh scripts/deliver-sql-columnar-ngram.sh scripts/format-sql-columnar-ngram.sh scripts/inspect-sql-analytics-primitives.sh scripts/inspect-sql-columnar-string-path.sh scripts/test-race-sql-columnar-ngram.sh scripts/test-sql-columnar-ngram.sh scripts/verify-sql-columnar-ngram-docs.sh'

if ! git diff --cached --quiet; then
	echo 'refusing to deliver with pre-existing staged changes' >&2
	exit 1
fi

git diff --check -- $paths
git add -- $paths
git diff --cached --check
git commit -m 'feat: prune columnar substring segments'
git push
