#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to stage: the index is not clean' >&2
	exit 1
fi

if git show HEAD:Makefile | grep -F '# CH038_OR_NULL_COLUMNAR_FEATURE_TARGETS_BEGIN' >/dev/null; then
	printf '%s\n' 'refusing to stage: CH038 Makefile block is already in HEAD' >&2
	exit 1
fi

makefile_block=$(mktemp)
base_makefile=$(mktemp)
candidate_makefile=$(mktemp)
makefile_patch=$(mktemp)
trap 'rm -f "$makefile_block" "$base_makefile" "$candidate_makefile" "$makefile_patch"' EXIT

awk '
/^# CH038_OR_NULL_COLUMNAR_FEATURE_TARGETS_BEGIN$/ { in_block = 1 }
in_block { print }
/^# CH038_OR_NULL_COLUMNAR_FEATURE_TARGETS_END$/ { exit }
' Makefile > "$makefile_block"
test -s "$makefile_block"

git show HEAD:Makefile > "$base_makefile"
cat "$base_makefile" "$makefile_block" > "$candidate_makefile"
set +e
diff -u --label a/Makefile --label b/Makefile "$base_makefile" "$candidate_makefile" > "$makefile_patch"
diff_status=$?
set -e
test "$diff_status" -eq 1
git apply --cached "$makefile_patch"

git add -- \
	BENCHMARK.md \
	CH038_OR_NULL_COLUMNAR.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	ENGINE_IDEAS.md \
	README.md \
	hat/hatSql/query.go \
	hat/hatSql/ch038_or_null_columnar_test.go \
	scripts/test-ch038-or-null-columnar.sh \
	scripts/test-ch038-or-null-columnar-package.sh \
	scripts/benchmark-ch038-or-null-columnar.sh \
	scripts/format-ch038-or-null-columnar.sh \
	scripts/race-ch038-or-null-columnar.sh \
	scripts/vet-ch038-or-null-columnar.sh \
	scripts/verify-ch038-or-null-columnar-docs.sh \
	scripts/review-ch038-or-null-columnar.sh \
	scripts/stage-ch038-or-null-columnar.sh \
	scripts/commit-ch038-or-null-columnar.sh \
	scripts/push-ch038-or-null-columnar.sh

git diff --cached --check
printf '%s\n' '--- staged feature ---'
git diff --cached --stat
git status --short
