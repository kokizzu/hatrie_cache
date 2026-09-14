#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to commit: the index already contains staged changes' >&2
	exit 1
fi

feature_paths=(
	Makefile
	README.md
	BENCHMARK.md
	ENGINE_IDEAS.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	MZ040_INCREMENTAL_PERCENTILE.md
	hat/hatSql/m040_incremental_percentile.go
	hat/hatSql/m040_incremental_percentile_test.go
	hat/hatSql/m040_incremental_percentile_benchmark_test.go
	scripts/test-mz040-incremental-percentile.sh
	scripts/benchmark-mz040-incremental-percentile.sh
	scripts/format-mz040-incremental-percentile.sh
	scripts/race-mz040-incremental-percentile.sh
	scripts/vet-mz040-incremental-percentile.sh
	scripts/review-mz040-incremental-percentile.sh
	scripts/verify-mz040-incremental-percentile.sh
	scripts/commit-mz040-incremental-percentile.sh
	scripts/push-mz040-incremental-percentile.sh
)

git add \
	README.md \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	MZ040_INCREMENTAL_PERCENTILE.md \
	hat/hatSql/m040_incremental_percentile.go \
	hat/hatSql/m040_incremental_percentile_test.go \
	hat/hatSql/m040_incremental_percentile_benchmark_test.go \
	scripts/test-mz040-incremental-percentile.sh \
	scripts/benchmark-mz040-incremental-percentile.sh \
	scripts/format-mz040-incremental-percentile.sh \
	scripts/race-mz040-incremental-percentile.sh \
	scripts/vet-mz040-incremental-percentile.sh \
	scripts/review-mz040-incremental-percentile.sh \
	scripts/verify-mz040-incremental-percentile.sh \
	scripts/commit-mz040-incremental-percentile.sh \
	scripts/push-mz040-incremental-percentile.sh

base=$(mktemp)
feature_makefile=$(mktemp)
patch_file=$(mktemp)
staged=$(mktemp)
expected=$(mktemp)
trap 'rm -f "$base" "$feature_makefile" "$patch_file" "$staged" "$expected"' EXIT

git show HEAD:Makefile >"$base"
cp "$base" "$feature_makefile"
printf '\n%s\n' \
'.PHONY: test-mz040-incremental-percentile benchmark-mz040-incremental-percentile format-mz040-incremental-percentile race-mz040-incremental-percentile vet-mz040-incremental-percentile review-mz040-incremental-percentile verify-mz040-incremental-percentile commit-mz040-incremental-percentile push-mz040-incremental-percentile' \
'test-mz040-incremental-percentile:' \
$'\t@bash scripts/test-mz040-incremental-percentile.sh' \
'benchmark-mz040-incremental-percentile:' \
$'\t@bash scripts/benchmark-mz040-incremental-percentile.sh' \
'format-mz040-incremental-percentile:' \
$'\t@bash scripts/format-mz040-incremental-percentile.sh' \
'race-mz040-incremental-percentile:' \
$'\t@bash scripts/race-mz040-incremental-percentile.sh' \
'vet-mz040-incremental-percentile:' \
$'\t@bash scripts/vet-mz040-incremental-percentile.sh' \
'review-mz040-incremental-percentile:' \
$'\t@bash scripts/review-mz040-incremental-percentile.sh' \
'verify-mz040-incremental-percentile:' \
$'\t@bash scripts/verify-mz040-incremental-percentile.sh' \
'commit-mz040-incremental-percentile:' \
$'\t@bash scripts/commit-mz040-incremental-percentile.sh' \
'push-mz040-incremental-percentile:' \
$'\t@bash scripts/push-mz040-incremental-percentile.sh' >>"$feature_makefile"

diff -u "$base" "$feature_makefile" >"$patch_file" || diff_status=$?
if [[ ${diff_status:-0} -ne 1 ]]; then
	printf '%s\n' 'could not build the isolated Makefile patch' >&2
	exit 1
fi
sed -i '1s|^--- .*|--- a/Makefile|;2s|^+++ .*|+++ b/Makefile|' "$patch_file"
git apply --cached "$patch_file"

printf '%s\n' "${feature_paths[@]}" | sort >"$expected"
git diff --cached --name-only | sort >"$staged"
if ! cmp -s "$expected" "$staged"; then
	printf '%s\n' 'refusing to commit: staged paths are not limited to MZ-040' >&2
	git diff --cached --name-only >&2
	exit 1
fi
git diff --cached --check
git commit -m 'feat(hatSql): add incremental percentile operator'
