#!/usr/bin/env bash
set -euo pipefail

paths=(
	Makefile
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

if ! git diff --cached --quiet; then
	echo 'refusing to stage CH-U15 with an existing index'
	exit 1
fi

base_file=$(mktemp)
candidate_file=$(mktemp)
patch_file=$(mktemp)
trap 'rm -f "$base_file" "$candidate_file" "$patch_file"' EXIT

git show HEAD:Makefile > "$base_file"
cp "$base_file" "$candidate_file"
awk '
	/^# CHU15 (red\/green|format|benchmark|verification|review|delivery) targets$/ {capture=1}
	capture {print}
	/^# End CHU15 (red\/green|format|benchmark|verification|review|delivery) targets$/ {capture=0}
' Makefile >> "$candidate_file"
diff -u "$base_file" "$candidate_file" > "$patch_file" || test $? -eq 1
sed -i '1s|^--- .*|--- a/Makefile|; 2s|^+++ .*|+++ b/Makefile|' "$patch_file"
git apply --cached "$patch_file"

git add -- \
	hat/hatSql/decimal_kernels.go \
	hat/hatSql/decimal_types.go \
	hat/hatSql/ch_u15_decimal_kernels_test.go \
	hat/hatSql/ch_u15_decimal_baseline_benchmark_test.go \
	hat/hatSql/ch_u15_decimal_kernels_benchmark_test.go \
	CHU15_VECTORIZED_DECIMAL_KERNELS.md \
	README.md \
	BENCHMARK.md \
	PRODUCT_IDEA_GAPS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	scripts/test-chu15-c255.sh \
	scripts/format-chu15-c255.sh \
	scripts/benchmark-chu15-before-c255.sh \
	scripts/benchmark-chu15-c255.sh \
	scripts/test-chu15-package-clean-c255.sh \
	scripts/race-chu15-clean-c255.sh \
	scripts/vet-chu15-clean-c255.sh \
	scripts/verify-chu15-docs-c255.sh \
	scripts/review-chu15-c255.sh \
	scripts/stage-chu15-c255.sh \
	scripts/commit-chu15-c255.sh \
	scripts/push-chu15-c255.sh

staged_paths=$(git diff --cached --name-only | sort)
expected_paths=$(printf '%s\n' "${paths[@]}" | sort)
if test "$staged_paths" != "$expected_paths"; then
	echo 'staged path set does not match the CH-U15 allowlist'
	diff -u <(printf '%s\n' "$expected_paths") <(printf '%s\n' "$staged_paths") || true
	exit 1
fi
git diff --cached --check
