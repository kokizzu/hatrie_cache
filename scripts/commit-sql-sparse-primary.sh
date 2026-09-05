#!/bin/sh
set -eu

if [ -n "$(git diff --cached --name-only)" ]; then
	printf '%s\n' 'refusing to commit: the real index already has staged changes' >&2
	exit 1
fi

temporary_directory=$(mktemp -d)
cleanup() {
	rm -rf "$temporary_directory"
}
trap cleanup EXIT HUP INT TERM

query_patch="$temporary_directory/query.patch"
git diff --unified=3 -- hat/hatSql/query.go | awk '
/^diff --git / { print; header = 1; keep = 0; next }
header && (/^index / || /^--- / || /^\+\+\+ /) { print; next }
/^@@ / {
	header = 0
	keep = $0 ~ /^@@ -8710,/ || $0 ~ /^@@ -8978,/ || $0 ~ /^@@ -9046,/
	if (keep) {
		print
	}
	next
}
keep { print }
' > "$query_patch"
if [ ! -s "$query_patch" ]; then
	printf '%s\n' 'refusing to commit: sparse query hunks were not found' >&2
	exit 1
fi

git show HEAD:Makefile > "$temporary_directory/Makefile"
awk '
/^audit-extensibility-goal:/ && !inserted {
	print ".PHONY: test-sql-sparse-primary"
	print "test-sql-sparse-primary:"
	print "\tsh ./scripts/test-sql-sparse-primary.sh"
	print ""
	print ".PHONY: format-sql-sparse-primary"
	print "format-sql-sparse-primary:"
	print "\tsh ./scripts/format-sql-sparse-primary.sh"
	print ""
	print ".PHONY: benchmark-sql-sparse-primary"
	print "benchmark-sql-sparse-primary:"
	print "\tsh ./scripts/benchmark-sql-sparse-primary.sh"
	print ""
	print ".PHONY: commit-sql-sparse-primary"
	print "commit-sql-sparse-primary:"
	print "\tsh ./scripts/commit-sql-sparse-primary.sh"
	print ""
	inserted = 1
}
{ print }
END {
	if (!inserted) {
		exit 1
	}
}
' "$temporary_directory/Makefile" > "$temporary_directory/Makefile.new"
mv "$temporary_directory/Makefile.new" "$temporary_directory/Makefile"

export GIT_INDEX_FILE="$temporary_directory/index"
git read-tree HEAD

makefile_blob=$(git hash-object -w "$temporary_directory/Makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git apply --cached --whitespace=error "$query_patch"
git add -- \
	BENCHMARK.md \
	INSPIRATION.md \
	README.md \
	hat/hatSql/columnar_segment_skip_test.go \
	hat/hatSql/contracts.go \
	hat/hatSql/sparse_primary_index_benchmark_test.go \
	hat/hatSql/sparse_primary_index_test.go \
	hat/hatSql/typed_table.go \
	hat/hatSql/typed_table_sparse_primary_test.go \
	scripts/benchmark-sql-sparse-primary.sh \
	scripts/commit-sql-sparse-primary.sh \
	scripts/format-sql-sparse-primary.sh \
	scripts/test-sql-sparse-primary.sh

git diff --cached --check
git diff --cached --name-only
git commit -m 'sql: add sparse primary mark pruning'
git push origin HEAD

unset GIT_INDEX_FILE
for path in \
	BENCHMARK.md \
	INSPIRATION.md \
	Makefile \
	README.md \
	hat/hatSql/columnar_segment_skip_test.go \
	hat/hatSql/contracts.go \
	hat/hatSql/query.go \
	hat/hatSql/sparse_primary_index_benchmark_test.go \
	hat/hatSql/sparse_primary_index_test.go \
	hat/hatSql/typed_table.go \
	hat/hatSql/typed_table_sparse_primary_test.go \
	scripts/benchmark-sql-sparse-primary.sh \
	scripts/commit-sql-sparse-primary.sh \
	scripts/format-sql-sparse-primary.sh \
	scripts/test-sql-sparse-primary.sh
do
	blob=$(git rev-parse "HEAD:$path")
	git update-index --add --cacheinfo "100644,$blob,$path"
done
