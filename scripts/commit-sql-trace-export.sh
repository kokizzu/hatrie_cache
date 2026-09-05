#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

if ! git diff --cached --quiet; then
    printf '%s\n' 'refusing to deliver while unrelated changes are staged' >&2
    git diff --cached --name-only >&2
    exit 1
fi

makefile_head=$(mktemp)
makefile_with_trace=$(mktemp)
api_head=$(mktemp)
api_with_trace=$(mktemp)
index_file=$(mktemp)
rm -f "$index_file"
trap 'rm -f "$makefile_head" "$makefile_with_trace" "$api_head" "$api_with_trace" "$index_file"' EXIT HUP INT TERM

GIT_INDEX_FILE="$index_file" git read-tree HEAD
export GIT_INDEX_FILE

git show HEAD:Makefile > "$makefile_head"
awk '
BEGIN { inserted = 0 }
/^audit-extensibility-goal:/ {
    if (inserted == 0) {
        print "test-sql-trace-export:"
        print "\tsh ./scripts/test-sql-trace-export.sh"
        print ""
        print "format-sql-trace-export:"
        print "\tsh ./scripts/format-sql-trace-export.sh"
        print ""
        print "benchmark-sql-trace-export:"
        print "\tsh ./scripts/benchmark-sql-trace-export.sh"
        print ""
        print "commit-sql-trace-export:"
        print "\tsh ./scripts/commit-sql-trace-export.sh"
        print ""
        inserted = 1
    }
}
{ print }
END {
    if (inserted == 0) {
        exit 1
    }
}
' "$makefile_head" > "$makefile_with_trace"

git show HEAD:api.go > "$api_head"
awk '
/^type SQLQueryOperator = core\.SQLQueryOperator$/ {
    print
    print "type SQLQueryTraceRecorder = core.SQLQueryTraceRecorder"
    next
}
/^var NewSQLResultCache = core\.NewSQLResultCache$/ {
    print
    print "var NewSQLQueryTraceRecorder = core.NewSQLQueryTraceRecorder"
    next
}
{ print }
' "$api_head" > "$api_with_trace"

makefile_blob=$(git hash-object -w "$makefile_with_trace")
api_blob=$(git hash-object -w "$api_with_trace")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git update-index --add --cacheinfo "100644,$api_blob,api.go"
git add -- \
    BENCHMARK.md \
    INSPIRATION.md \
    README.md \
    hat/hatCache/sql_query.go \
    hat/hatSql/query_trace.go \
    hat/hatSql/query_trace_benchmark_test.go \
    hat/hatSql/query_trace_test.go \
    scripts/benchmark-sql-trace-export.sh \
    scripts/commit-sql-trace-export.sh \
    scripts/format-sql-trace-export.sh \
    scripts/test-sql-trace-export.sh

expected='BENCHMARK.md
INSPIRATION.md
Makefile
README.md
api.go
hat/hatCache/sql_query.go
hat/hatSql/query_trace.go
hat/hatSql/query_trace_benchmark_test.go
hat/hatSql/query_trace_test.go
scripts/benchmark-sql-trace-export.sh
scripts/commit-sql-trace-export.sh
scripts/format-sql-trace-export.sh
scripts/test-sql-trace-export.sh'
actual=$(git diff --cached --name-only)
if [ "$actual" != "$expected" ]; then
    printf '%s\n' "unexpected staged paths:" >&2
    printf '%s\n' "$actual" >&2
    exit 1
fi

git diff --cached --check
if git diff --cached --quiet; then
    printf '%s\n' 'no SQL trace export changes staged' >&2
    exit 1
fi

git commit -m 'feat: add SQL query trace export'
git push origin master

unset GIT_INDEX_FILE
for path in \
    BENCHMARK.md \
    INSPIRATION.md \
    Makefile \
    README.md \
    api.go \
    hat/hatCache/sql_query.go \
    hat/hatSql/query_trace.go \
    hat/hatSql/query_trace_benchmark_test.go \
    hat/hatSql/query_trace_test.go \
    scripts/benchmark-sql-trace-export.sh \
    scripts/commit-sql-trace-export.sh \
    scripts/format-sql-trace-export.sh \
    scripts/test-sql-trace-export.sh
do
    blob=$(git rev-parse "HEAD:$path")
    git update-index --add --cacheinfo "100644,$blob,$path"
done
