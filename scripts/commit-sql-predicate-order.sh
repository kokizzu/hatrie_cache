#!/bin/sh
set -eu

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to commit with pre-existing staged changes' >&2
	exit 1
fi

feature_files='BENCHMARK.md INSPIRATION.md README.md hat/hatSql/numeric_predicate_order.go hat/hatSql/numeric_predicate_order_test.go hat/hatSql/numeric_predicate_order_benchmark_test.go scripts/benchmark-sql-predicate-order.sh scripts/format-sql-predicate-order.sh scripts/test-sql-predicate-order.sh scripts/commit-sql-predicate-order.sh'
temporary_index=$(mktemp)
temporary_makefile=$(mktemp)
temporary_makefile_base=$(mktemp)
temporary_makefile_block=$(mktemp)
temporary_query_patch=$(mktemp)
rm -f "$temporary_index"
cleanup() {
	rm -f "$temporary_index" "$temporary_makefile" "$temporary_makefile_base" "$temporary_makefile_block" "$temporary_query_patch"
}
trap cleanup EXIT

export GIT_INDEX_FILE="$temporary_index"
git read-tree -i -m HEAD

printf '%s\n' \
	'.PHONY: test-sql-predicate-order' \
	'test-sql-predicate-order:' \
	'\tsh ./scripts/test-sql-predicate-order.sh' \
	'' \
	'.PHONY: benchmark-sql-predicate-order' \
	'benchmark-sql-predicate-order:' \
	'\tsh ./scripts/benchmark-sql-predicate-order.sh' \
	'' \
	'.PHONY: format-sql-predicate-order' \
	'format-sql-predicate-order:' \
	'\tsh ./scripts/format-sql-predicate-order.sh' \
	'' \
	'.PHONY: commit-sql-predicate-order' \
	'commit-sql-predicate-order:' \
	'\tsh ./scripts/commit-sql-predicate-order.sh' > "$temporary_makefile_block"
git show HEAD:Makefile > "$temporary_makefile_base"
awk -v block_file="$temporary_makefile_block" '
$0 == "audit-extensibility-goal:" {
	while ((getline line < block_file) > 0) {
		print line
	}
	close(block_file)
}
{ print }
' "$temporary_makefile_base" > "$temporary_makefile"
makefile_object=$(git hash-object -w "$temporary_makefile")
git update-index --add --cacheinfo "100644,$makefile_object,Makefile"

for path in $feature_files; do
	git add -- "$path"
done

{
	git diff --unified=3 -- hat/hatSql/query.go | awk '
NR <= 4 { print; next }
/^@@/ {
	if (block != "" && keep) {
		printf "%s", block
	}
	block = $0 "\n"
	keep = 0
	next
}
{
	block = block $0 "\n"
	if ($0 ~ /sqlColumnarOrderNumericPredicates/) {
		keep = 1
	}
}
END {
	if (block != "" && keep) {
		printf "%s", block
	}
}'
} > "$temporary_query_patch"
if ! rg -q 'sqlColumnarOrderNumericPredicates' "$temporary_query_patch"; then
	printf '%s\n' 'predicate-order query patch was not found' >&2
	exit 1
fi
git apply --cached "$temporary_query_patch"

git commit -m 'sql: reorder numeric predicates by selectivity'
git push origin HEAD

unset GIT_INDEX_FILE
for path in $feature_files hat/hatSql/query.go Makefile; do
	object=$(git rev-parse "HEAD:$path")
	git update-index --add --cacheinfo "100644,$object,$path"
done
