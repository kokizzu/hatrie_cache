#!/usr/bin/env bash
set -euo pipefail

files=(
	Makefile
	BENCHMARK.md
	DATA_STRUCTURE.md
	PRODUCT_IDEA_GAPS.md
	README.md
	TU22_CROSS_INDEX_UNIQUE_CONSTRAINTS.md
	hat/hatDataStructure/unique_constraint_set.go
	hat/hatDataStructure/t_u22_unique_constraint_set_test.go
	hat/hatDataStructure/t_u22_unique_constraint_set_benchmark_test.go
	scripts/format-t-u22.sh
	scripts/test-t-u22-red.sh
	scripts/benchmark-t-u22-baseline.sh
	scripts/test-t-u22.sh
	scripts/benchmark-t-u22.sh
	scripts/verify-t-u22.sh
	scripts/review-t-u22.sh
	scripts/deliver-t-u22.sh
)

case "${1:-}" in
	stage)
		git add -- "${files[@]}"
		git diff --cached --check
		git status --short
		;;
	commit)
		git commit -m "feat: add atomic cross-index unique constraints [skip ci]"
		;;
	push)
		git push -u origin HEAD
		;;
	*)
		printf 'usage: %s {stage|commit|push}\n' "$0" >&2
		exit 2
		;;
esac
