#!/usr/bin/env bash
set -euo pipefail

expected=(
	Makefile
	README.md
	PRODUCT_IDEA_GAPS.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	CHU08_AUTOMATIC_SQL_RESULT_CACHE.md
	hat/hatCache/main.go
	hat/hatCache/sql_query.go
	hat/hatCache/sql_result_cache_auto.go
	hat/hatCache/ch008_auto_result_cache_test.go
	hat/hatCache/ch008_auto_result_cache_benchmark_test.go
	scripts/test-chu08-c248.sh
	scripts/benchmark-chu08-c248.sh
	scripts/format-chu08-c248.sh
	scripts/test-chu08-package-c248.sh
	scripts/test-chu08-repo-c248.sh
	scripts/race-chu08-c248.sh
	scripts/vet-chu08-c248.sh
	scripts/verify-chu08-docs-c248.sh
	scripts/review-chu08-c248.sh
	scripts/stage-chu08-c248.sh
	scripts/commit-chu08-c248.sh
	scripts/push-chu08-c248.sh
)

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu08-commit.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT
printf '%s\n' "${expected[@]}" | sort > "$tmp_dir/expected"
git diff --cached --name-only | sort > "$tmp_dir/actual"
if ! cmp -s "$tmp_dir/expected" "$tmp_dir/actual"; then
	printf '%s\n' 'refusing to commit: staged paths do not match CH-U08 allowlist' >&2
	diff -u "$tmp_dir/expected" "$tmp_dir/actual" >&2 || true
	exit 1
fi
git diff --cached --check
git commit -m 'feat: automatic SQL result cache wiring [skip ci]'
