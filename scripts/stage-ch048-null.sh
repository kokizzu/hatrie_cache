#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	echo "refusing to stage CH048 NULL feature with a non-empty index" >&2
	exit 1
fi

temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-ch048-null-stage.XXXXXX")
cleanup() {
	rm -rf "$temp_dir"
}
trap cleanup EXIT

block="$temp_dir/feature-targets"
base="$temp_dir/base-Makefile"
candidate="$temp_dir/candidate-Makefile"
raw_patch="$temp_dir/Makefile.patch.raw"
patch="$temp_dir/Makefile.patch"

awk '/^# CH048_NULL_FEATURE_TARGETS_BEGIN$/{capture=1} capture{print} /^# CH048_NULL_FEATURE_TARGETS_END$/{exit}' Makefile > "$block"
if [[ ! -s "$block" ]]; then
	echo "CH048 NULL Makefile target block is missing" >&2
	exit 1
fi

git show HEAD:Makefile > "$base"
{
	cat "$base"
	printf '\n'
	cat "$block"
} > "$candidate"

diff -u "$base" "$candidate" > "$raw_patch" || true
{
	printf '%s\n' '--- a/Makefile'
	printf '%s\n' '+++ b/Makefile'
	tail -n +3 "$raw_patch"
} > "$patch"
git apply --cached "$patch"

git add \
	BENCHMARK.md \
	CH048_NULL_PREDICATE.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	ENGINE_IDEAS.md \
	hat/hatSql/columnar_null_predicate.go \
	hat/hatSql/ch048_null_predicate_test.go \
	hat/hatSql/query.go \
	scripts/benchmark-ch048-null.sh \
	scripts/commit-ch048-null.sh \
	scripts/format-ch048-null.sh \
	scripts/push-ch048-null.sh \
	scripts/race-ch048-null.sh \
	scripts/review-ch048-null.sh \
	scripts/stage-ch048-null.sh \
	scripts/test-ch048-null-package.sh \
	scripts/test-ch048-null.sh \
	scripts/verify-ch048-null-docs.sh \
	scripts/vet-ch048-null.sh

git diff --cached --check
git diff --cached --name-status
