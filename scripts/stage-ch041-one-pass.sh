#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CH041_GROUPING_PLAN_SHARING.md \
	ENGINE_IDEAS.md \
	hat/hatSql/ch041_one_pass_grouping.go \
	hat/hatSql/ch041_one_pass_grouping_benchmark_test.go \
	hat/hatSql/ch041_one_pass_grouping_test.go \
	hat/hatSql/grouping_sets.go \
	hat/hatSql/grouping_sets_test.go \
	hat/hatSql/query.go \
	scripts/benchmark-ch041-one-pass.sh \
	scripts/commit-ch041-one-pass.sh \
	scripts/format-ch041-one-pass.sh \
	scripts/push-ch041-one-pass.sh \
	scripts/stage-ch041-one-pass.sh \
	scripts/test-ch041-one-pass-results.sh \
	scripts/test-ch041-one-pass.sh

temporary_directory=$(mktemp -d)
trap 'rm -rf "$temporary_directory"' EXIT
git show HEAD:Makefile >"$temporary_directory/base"
{
	cat "$temporary_directory/base"
	printf '%s\n' \
		'' \
		'.PHONY: test-ch041-one-pass' \
		'test-ch041-one-pass:' \
		$'\tbash ./scripts/test-ch041-one-pass.sh' \
		'' \
		'.PHONY: format-ch041-one-pass' \
		'format-ch041-one-pass:' \
		$'\tbash ./scripts/format-ch041-one-pass.sh' \
		'' \
		'.PHONY: test-ch041-one-pass-results' \
		'test-ch041-one-pass-results:' \
		$'\tbash ./scripts/test-ch041-one-pass-results.sh' \
		'' \
		'.PHONY: benchmark-ch041-one-pass' \
		'benchmark-ch041-one-pass:' \
		$'\tbash ./scripts/benchmark-ch041-one-pass.sh' \
		'' \
		'.PHONY: stage-ch041-one-pass' \
		'stage-ch041-one-pass:' \
		$'\tbash ./scripts/stage-ch041-one-pass.sh' \
		'' \
		'.PHONY: commit-ch041-one-pass' \
		'commit-ch041-one-pass:' \
		$'\tbash ./scripts/commit-ch041-one-pass.sh' \
		'' \
		'.PHONY: push-ch041-one-pass' \
		'push-ch041-one-pass:' \
		$'\tbash ./scripts/push-ch041-one-pass.sh'
} >"$temporary_directory/desired"
git diff --no-index --src-prefix=a/ --dst-prefix=b/ "$temporary_directory/base" "$temporary_directory/desired" >"$temporary_directory/makefile.patch" || status=$?
if [ "${status:-0}" -gt 1 ]; then
	cat "$temporary_directory/makefile.patch" >&2
	exit "$status"
fi
sed -i "s|$temporary_directory/base|a/Makefile|g; s|$temporary_directory/desired|b/Makefile|g" "$temporary_directory/makefile.patch"
git apply --cached "$temporary_directory/makefile.patch"
