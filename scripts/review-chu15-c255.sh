#!/usr/bin/env bash
set -euo pipefail

paths=(
	hat/hatSql/decimal_kernels.go
	hat/hatSql/decimal_types.go
	hat/hatSql/ch_u15_decimal_kernels_test.go
	hat/hatSql/ch_u15_decimal_baseline_benchmark_test.go
	hat/hatSql/ch_u15_decimal_kernels_benchmark_test.go
	CHU15_VECTORIZED_DECIMAL_KERNELS.md
	README.md
	BENCHMARK.md
	PRODUCT_IDEA_GAPS.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	scripts/test-chu15-c255.sh
	scripts/format-chu15-c255.sh
	scripts/benchmark-chu15-before-c255.sh
	scripts/benchmark-chu15-c255.sh
	scripts/test-chu15-package-clean-c255.sh
	scripts/race-chu15-clean-c255.sh
	scripts/vet-chu15-clean-c255.sh
	scripts/verify-chu15-docs-c255.sh
	scripts/review-chu15-c255.sh
	scripts/stage-chu15-c255.sh
	scripts/commit-chu15-c255.sh
	scripts/push-chu15-c255.sh
)

gofmt -d \
	hat/hatSql/decimal_kernels.go \
	hat/hatSql/decimal_types.go \
	hat/hatSql/ch_u15_decimal_kernels_test.go \
	hat/hatSql/ch_u15_decimal_baseline_benchmark_test.go \
	hat/hatSql/ch_u15_decimal_kernels_benchmark_test.go
git diff --check -- "${paths[@]}"
test "$(rg -n '# CHU15 red/green targets' Makefile | wc -l)" = 1
test "$(rg -n '# CHU15 verification targets' Makefile | wc -l)" = 1
test "$(rg -n '# CHU15 delivery targets' Makefile | wc -l)" = 1
test "$(rg -n 'inspect-chu15-c254|status-chu15-c255' Makefile | wc -l || true)" = 0
git status --short -- "${paths[@]}"
git diff --stat -- "${paths[@]}"
