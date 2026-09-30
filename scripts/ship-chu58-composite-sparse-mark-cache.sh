#!/usr/bin/env bash
set -euo pipefail

mode=${1:-all}
files=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	CHU18_COMPOSITE_PRIMARY_MARK_PRUNING.md
	Makefile
	TYPED_TABLE_SPARSE_MARK_CACHE.md
	hat/hatSql/typed_table_sparse_mark_cache.go
	hat/hatSql/ch_u58_composite_sparse_mark_cache_test.go
	hat/hatSql/ch_u58_composite_sparse_mark_cache_benchmark_test.go
	scripts/benchmark-chu58-composite-sparse-mark-cache.sh
	scripts/format-chu58-composite-sparse-mark-cache.sh
	scripts/ship-chu58-composite-sparse-mark-cache.sh
	scripts/test-chu58-composite-sparse-mark-cache.sh
	scripts/verify-chu58-composite-sparse-mark-cache.sh
)

stage() {
	git add "${files[@]}"
	git diff --cached --check
	git diff --cached --name-only
}

commit() {
	if git diff --cached --quiet -- "${files[@]}"; then
		printf '%s\n' 'no staged feature changes to commit'
		return
	fi
	git commit -m 'feat: retain composite sparse marks after layout eviction [skip ci]'
}

push() {
	git push origin HEAD
}

case "$mode" in
stage)
	stage
	;;
commit)
	commit
	;;
push)
	push
	;;
all)
	stage
	commit
	push
	;;
*)
	printf 'usage: %s [stage|commit|push|all]\n' "$0" >&2
	exit 2
	;;
esac
