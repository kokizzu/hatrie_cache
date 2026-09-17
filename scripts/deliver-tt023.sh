#!/bin/sh
set -eu

mode=${1:-stage}
paths='Makefile BENCHMARK.md ENGINE_IDEAS.md README.md TT023_STRING_HASH_FASTPATH.md hat/hatCache/sql_query.go hat/hatCache/sql_borrowed_index.go hat/hatCache/tt023_string_index_test.go hat/hatCache/tt023_string_index_benchmark_test.go scripts/test-tt023.sh scripts/deliver-tt023.sh'

case "$mode" in
stage)
	git add -- $paths
	;;
commit)
	git diff --cached --quiet -- $paths && {
		printf '%s\n' 'TT-023 has no staged changes' >&2
		exit 1
	}
	git commit -m 'perf(sql): remove string index lookup allocation [skip ci]'
	;;
push)
	git push origin HEAD:refs/heads/master
	;;
verify)
	git diff --cached --check
	git diff --cached --stat -- $paths
	git status --short --untracked-files=all
	;;
*)
	printf 'unknown TT-023 delivery mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
