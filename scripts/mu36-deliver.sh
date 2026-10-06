#!/usr/bin/env bash
set -euo pipefail

mode=${1:-status}
files=(
	BENCHMARK.md
	INSPIRATION.md
	PRODUCT_IDEA_GAPS.md
	MU036_HYDRATION_ADMISSION.md
	Makefile
	hat/hatSql/typed_table_arrangement_hydration_admission.go
	hat/hatSql/typed_table_arrangements.go
	hat/hatSql/typed_table_join_arrangements.go
	hat/hatSql/mu036_hydration_admission_test.go
	hat/hatSql/mu036_hydration_benchmark_test.go
	scripts/mu36-hydration.sh
	scripts/mu36-deliver.sh
)

case "$mode" in
status)
	git status --short -- "${files[@]}"
	git diff --check -- "${files[@]}"
	git diff --stat -- "${files[@]}"
	git diff --cached --check
	git diff --cached --stat
	;;
stage)
	git add -- "${files[@]}"
	git diff --cached --check
	git status --short -- "${files[@]}"
	;;
commit)
	git add -- "${files[@]}"
	git diff --cached --check
	git commit -m 'feat(sql): add hydration admission status [skip ci]'
	;;
push)
	git push -u origin codex/inspiration-mu36-hydration-20261006
	;;
*)
	printf 'usage: %s status|stage|commit|push\n' "$0" >&2
	exit 2
	;;
esac
