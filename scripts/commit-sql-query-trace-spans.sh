#!/usr/bin/env bash
set -eu

index_file=$(mktemp)
head_makefile=$(mktemp)
next_makefile=$(mktemp)
cleanup() {
	rm -f "$index_file" "$head_makefile" "$next_makefile"
}
trap cleanup EXIT
rm -f "$index_file"

GIT_INDEX_FILE="$index_file" git read-tree HEAD
git show HEAD:Makefile >"$head_makefile"
awk '
{
	print
}
END {
	print ".PHONY: test-sql-query-trace-spans"
	print "test-sql-query-trace-spans:"
	print "\tbash ./scripts/test-sql-query-trace-spans.sh"
	print ".PHONY: format-sql-query-trace-spans"
	print "format-sql-query-trace-spans:"
	print "\tbash ./scripts/format-sql-query-trace-spans.sh"
	print ".PHONY: benchmark-sql-query-trace-spans"
	print "benchmark-sql-query-trace-spans:"
	print "\tbash ./scripts/benchmark-sql-query-trace-spans.sh"
	print ".PHONY: test-race-sql-query-trace-spans"
	print "test-race-sql-query-trace-spans:"
	print "\tbash ./scripts/test-race-sql-query-trace-spans.sh"
	print ".PHONY: verify-sql-query-trace-spans"
	print "verify-sql-query-trace-spans:"
	print "\tbash ./scripts/verify-sql-query-trace-spans.sh"
	print ".PHONY: commit-sql-query-trace-spans"
	print "commit-sql-query-trace-spans:"
	print "\tbash ./scripts/commit-sql-query-trace-spans.sh"
}' "$head_makefile" >"$next_makefile"

GIT_INDEX_FILE="$index_file" git add -- ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INSPIRATION.md QUERY_TRACING.md README.md hat/hatSql/query_trace.go hat/hatSql/query_trace_spans.go hat/hatSql/query_trace_spans_benchmark_test.go hat/hatSql/query_trace_spans_test.go scripts/benchmark-sql-query-trace-spans.sh scripts/commit-sql-query-trace-spans.sh scripts/format-sql-query-trace-spans.sh scripts/test-race-sql-query-trace-spans.sh scripts/test-sql-query-trace-spans.sh scripts/verify-sql-query-trace-spans.sh
makefile_blob=$(git hash-object -w "$next_makefile")
GIT_INDEX_FILE="$index_file" git update-index --add --cacheinfo 100644,"$makefile_blob",Makefile

expected='ADOPTED_QUERY_ENGINE_IDEAS.md
BENCHMARK.md
INSPIRATION.md
Makefile
QUERY_TRACING.md
README.md
hat/hatSql/query_trace.go
hat/hatSql/query_trace_spans.go
hat/hatSql/query_trace_spans_benchmark_test.go
hat/hatSql/query_trace_spans_test.go
scripts/benchmark-sql-query-trace-spans.sh
scripts/commit-sql-query-trace-spans.sh
scripts/format-sql-query-trace-spans.sh
scripts/test-race-sql-query-trace-spans.sh
scripts/test-sql-query-trace-spans.sh
scripts/verify-sql-query-trace-spans.sh'
actual=$(GIT_INDEX_FILE="$index_file" git diff --cached --name-only)
if [ "$actual" != "$expected" ]; then
	printf '%s\n' 'unexpected SQL trace span staging:' >&2
	printf '%s\n' "$actual" >&2
	exit 1
fi

GIT_INDEX_FILE="$index_file" git diff --cached --check
GIT_INDEX_FILE="$index_file" git commit -m 'feat(sql): export query trace spans'
git push
