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
makefile_with_feature=$(mktemp)
api_head=$(mktemp)
api_with_feature=$(mktemp)
index_file=$(mktemp)
rm -f "$index_file"
trap 'rm -f "$makefile_head" "$makefile_with_feature" "$api_head" "$api_with_feature" "$index_file"' EXIT HUP INT TERM

GIT_INDEX_FILE="$index_file" git read-tree HEAD
export GIT_INDEX_FILE

git show HEAD:Makefile > "$makefile_head"
awk '
BEGIN { inserted = 0 }
/^audit-extensibility-goal:/ {
    if (inserted == 0) {
        print "test-sql-primary-order-advisor:"
        print "\tsh ./scripts/test-sql-primary-order-advisor.sh"
        print ""
        print "format-sql-primary-order-advisor:"
        print "\tsh ./scripts/format-sql-primary-order-advisor.sh"
        print ""
        print "benchmark-sql-primary-order-advisor:"
        print "\tsh ./scripts/benchmark-sql-primary-order-advisor.sh"
        print ""
        print "commit-sql-primary-order-advisor:"
        print "\tsh ./scripts/commit-sql-primary-order-advisor.sh"
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
' "$makefile_head" > "$makefile_with_feature"

git show HEAD:api.go > "$api_head"
awk '
/^type SQLQueryOperator = core\.SQLQueryOperator$/ {
    print
    print "type SQLPrimaryOrderRecommendation = core.SQLPrimaryOrderRecommendation"
    next
}
{ print }
' "$api_head" > "$api_with_feature"

makefile_blob=$(git hash-object -w "$makefile_with_feature")
api_blob=$(git hash-object -w "$api_with_feature")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git update-index --add --cacheinfo "100644,$api_blob,api.go"
git add -- \
    BENCHMARK.md \
    INSPIRATION.md \
    README.md \
    hat/hatCache/sql_query.go \
    hat/hatSql/index_advisor.go \
    hat/hatSql/index_advisor_primary_order_benchmark_test.go \
    hat/hatSql/index_advisor_primary_order_test.go \
    scripts/benchmark-sql-primary-order-advisor.sh \
    scripts/commit-sql-primary-order-advisor.sh \
    scripts/format-sql-primary-order-advisor.sh \
    scripts/test-sql-primary-order-advisor.sh

expected='BENCHMARK.md
INSPIRATION.md
Makefile
README.md
api.go
hat/hatCache/sql_query.go
hat/hatSql/index_advisor.go
hat/hatSql/index_advisor_primary_order_benchmark_test.go
hat/hatSql/index_advisor_primary_order_test.go
scripts/benchmark-sql-primary-order-advisor.sh
scripts/commit-sql-primary-order-advisor.sh
scripts/format-sql-primary-order-advisor.sh
scripts/test-sql-primary-order-advisor.sh'
actual=$(git diff --cached --name-only)
if [ "$actual" != "$expected" ]; then
    printf '%s\n' 'unexpected staged paths:' >&2
    printf '%s\n' "$actual" >&2
    exit 1
fi

git diff --cached --check
if git diff --cached --quiet; then
    printf '%s\n' 'no SQL primary-order advisor changes staged' >&2
    exit 1
fi

git commit -m 'feat: add SQL primary order advice'
git push origin master

unset GIT_INDEX_FILE
for path in \
    BENCHMARK.md \
    INSPIRATION.md \
    Makefile \
    README.md \
    api.go \
    hat/hatCache/sql_query.go \
    hat/hatSql/index_advisor.go \
    hat/hatSql/index_advisor_primary_order_benchmark_test.go \
    hat/hatSql/index_advisor_primary_order_test.go \
    scripts/benchmark-sql-primary-order-advisor.sh \
    scripts/commit-sql-primary-order-advisor.sh \
    scripts/format-sql-primary-order-advisor.sh \
    scripts/test-sql-primary-order-advisor.sh
do
    blob=$(git rev-parse "HEAD:$path")
    git update-index --add --cacheinfo "100644,$blob,$path"
done
