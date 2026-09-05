#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to commit with pre-existing staged changes' >&2
	exit 1
fi

feature_files=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	INSPIRATION.md
	README.md
	hat/hatSql/rewrite.go
	hat/hatSql/common_subexpression_test.go
	hat/hatSql/common_subexpression_benchmark_test.go
	scripts/benchmark-sql-cse.sh
	scripts/format-sql-cse.sh
	scripts/test-sql-cse.sh
	scripts/test-sql-cse-race.sh
	scripts/commit-sql-cse.sh
)

temporary_directory=$(mktemp -d)
temporary_index="$temporary_directory/index"
temporary_makefile="$temporary_directory/Makefile"
temporary_makefile_base="$temporary_directory/Makefile.base"
temporary_makefile_block="$temporary_directory/Makefile.block"
cleanup() {
	rm -rf "$temporary_directory"
}
trap cleanup EXIT

export GIT_INDEX_FILE="$temporary_index"
git read-tree -i -m HEAD

printf '%s\n' \
	'.PHONY: test-sql-cse' \
	'test-sql-cse:' \
	'\tbash ./scripts/test-sql-cse.sh' \
	'' \
	'.PHONY: benchmark-sql-cse' \
	'benchmark-sql-cse:' \
	'\tbash ./scripts/benchmark-sql-cse.sh' \
	'' \
	'.PHONY: format-sql-cse' \
	'format-sql-cse:' \
	'\tbash ./scripts/format-sql-cse.sh' \
	'' \
	'.PHONY: test-sql-cse-race' \
	'test-sql-cse-race:' \
	'\tbash ./scripts/test-sql-cse-race.sh' \
	'' \
	'.PHONY: commit-sql-cse' \
	'commit-sql-cse:' \
	'\tbash ./scripts/commit-sql-cse.sh' > "$temporary_makefile_block"
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
if ! rg -q '^test-sql-cse:$' "$temporary_makefile"; then
	printf '%s\n' 'failed to build feature Makefile' >&2
	exit 1
fi
makefile_object=$(git hash-object -w "$temporary_makefile")
git update-index --add --cacheinfo "100644,$makefile_object,Makefile"

for path in "${feature_files[@]}"; do
	git add -- "$path"
done

expected='ADOPTED_QUERY_ENGINE_IDEAS.md
BENCHMARK.md
INSPIRATION.md
Makefile
README.md
hat/hatSql/common_subexpression_benchmark_test.go
hat/hatSql/common_subexpression_test.go
hat/hatSql/rewrite.go
scripts/benchmark-sql-cse.sh
scripts/commit-sql-cse.sh
scripts/format-sql-cse.sh
scripts/test-sql-cse-race.sh
scripts/test-sql-cse.sh'
actual=$(git diff --cached --name-only)
if [ "$actual" != "$expected" ]; then
	printf '%s\n' 'unexpected staged paths:' >&2
	printf '%s\n' "$actual" >&2
	exit 1
fi
git diff --cached --check

git commit -m 'sql: eliminate duplicate pure predicates'
git push origin HEAD

unset GIT_INDEX_FILE
for path in "${feature_files[@]}" Makefile; do
	object=$(git rev-parse "HEAD:$path")
	git update-index --add --cacheinfo "100644,$object,$path"
done
