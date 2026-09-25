#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	MZ035_INCREMENTAL_MULTISET.md \
	hat/hatSql/mz035_incremental_multiset.go \
	hat/hatSql/mz035_incremental_multiset_baseline_benchmark_test.go \
	hat/hatSql/mz035_incremental_multiset_benchmark_test.go \
	hat/hatSql/mz035_incremental_multiset_test.go \
	scripts/benchmark-mz035-multiset-after.sh \
	scripts/benchmark-mz035-multiset-before.sh \
	scripts/commit-mz035-multiset.sh \
	scripts/format-mz035-multiset.sh \
	scripts/push-mz035-multiset.sh \
	scripts/race-mz035-multiset.sh \
	scripts/stage-mz035-multiset.sh \
	scripts/test-mz035-multiset.sh \
	scripts/vet-mz035-multiset.sh

temporary_directory=$(mktemp -d)
trap 'rm -rf "$temporary_directory"' EXIT
git show HEAD:Makefile >"$temporary_directory/base"
{
	cat "$temporary_directory/base"
	printf '%s\n' \
		'' \
		'benchmark-mz035-multiset-before:' \
		$'\tbash ./scripts/benchmark-mz035-multiset-before.sh' \
		'' \
		'benchmark-mz035-multiset-after:' \
		$'\tbash ./scripts/benchmark-mz035-multiset-after.sh' \
		'' \
		'race-mz035-multiset:' \
		$'\tbash ./scripts/race-mz035-multiset.sh' \
		'' \
		'vet-mz035-multiset:' \
		$'\tbash ./scripts/vet-mz035-multiset.sh' \
		'' \
		'test-mz035-multiset:' \
		$'\tbash ./scripts/test-mz035-multiset.sh' \
		'' \
		'format-mz035-multiset:' \
		$'\tbash ./scripts/format-mz035-multiset.sh' \
		'' \
		'stage-mz035-multiset:' \
		$'\tbash ./scripts/stage-mz035-multiset.sh' \
		'' \
		'commit-mz035-multiset:' \
		$'\tbash ./scripts/commit-mz035-multiset.sh' \
		'' \
		'push-mz035-multiset:' \
		$'\tbash ./scripts/push-mz035-multiset.sh'
} >"$temporary_directory/desired"
git diff --no-index --src-prefix=a/ --dst-prefix=b/ "$temporary_directory/base" "$temporary_directory/desired" >"$temporary_directory/makefile.patch" || status=$?
if [ "${status:-0}" -gt 1 ]; then
	cat "$temporary_directory/makefile.patch" >&2
	exit "$status"
fi
sed -i "s|$temporary_directory/base|a/Makefile|g; s|$temporary_directory/desired|b/Makefile|g" "$temporary_directory/makefile.patch"
git apply --cached "$temporary_directory/makefile.patch"
