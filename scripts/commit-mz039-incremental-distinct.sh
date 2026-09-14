#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	ENGINE_IDEAS.md
	MZ039_INCREMENTAL_DISTINCT.md
	README.md
	hat/hatSql/m039_incremental_distinct.go
	hat/hatSql/m039_incremental_distinct_benchmark_test.go
	hat/hatSql/m039_incremental_distinct_test.go
	scripts/benchmark-mz039-incremental-distinct.sh
	scripts/commit-mz039-incremental-distinct.sh
	scripts/format-mz039-incremental-distinct.sh
	scripts/push-mz039-incremental-distinct.sh
	scripts/race-mz039-incremental-distinct.sh
	scripts/review-mz039-incremental-distinct.sh
	scripts/test-mz039-incremental-distinct.sh
	scripts/verify-mz039-incremental-distinct.sh
	scripts/vet-mz039-incremental-distinct.sh
)

git add "${feature_paths[@]}"

base_file=$(mktemp)
feature_makefile=$(mktemp)
makefile_patch=$(mktemp)
expected_paths=$(mktemp)
staged_paths=$(mktemp)
cleanup() {
	rm -f "$base_file" "$feature_makefile" "$makefile_patch" "$expected_paths" "$staged_paths"
}
trap cleanup EXIT

git show HEAD:Makefile > "$base_file"
cp "$base_file" "$feature_makefile"
printf '\n%s\n' '.PHONY: test-mz039-incremental-distinct benchmark-mz039-incremental-distinct format-mz039-incremental-distinct race-mz039-incremental-distinct vet-mz039-incremental-distinct review-mz039-incremental-distinct verify-mz039-incremental-distinct' >> "$feature_makefile"
printf '%s\n' 'test-mz039-incremental-distinct:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/test-mz039-incremental-distinct.sh' >> "$feature_makefile"
printf '%s\n' 'benchmark-mz039-incremental-distinct:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/benchmark-mz039-incremental-distinct.sh' >> "$feature_makefile"
printf '%s\n' 'format-mz039-incremental-distinct:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/format-mz039-incremental-distinct.sh' >> "$feature_makefile"
printf '%s\n' 'race-mz039-incremental-distinct:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/race-mz039-incremental-distinct.sh' >> "$feature_makefile"
printf '%s\n' 'vet-mz039-incremental-distinct:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/vet-mz039-incremental-distinct.sh' >> "$feature_makefile"
printf '%s\n' 'review-mz039-incremental-distinct:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/review-mz039-incremental-distinct.sh' >> "$feature_makefile"
printf '%s\n' 'verify-mz039-incremental-distinct:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/verify-mz039-incremental-distinct.sh' >> "$feature_makefile"
printf '%s\n' 'commit-mz039-incremental-distinct:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/commit-mz039-incremental-distinct.sh' >> "$feature_makefile"
printf '%s\n' 'push-mz039-incremental-distinct:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/push-mz039-incremental-distinct.sh' >> "$feature_makefile"

set +e
diff -u "$base_file" "$feature_makefile" > "$makefile_patch"
diff_status=$?
set -e
if [[ $diff_status -ne 1 ]]; then
	printf 'failed to build isolated Makefile patch (status %d)\n' "$diff_status" >&2
	exit 1
fi
sed -i '1s|^--- .*|--- a/Makefile|; 2s|^+++ .*|+++ b/Makefile|' "$makefile_patch"
git apply --cached "$makefile_patch"

printf '%s\n' Makefile "${feature_paths[@]}" | sort > "$expected_paths"
git diff --cached --name-only | sort > "$staged_paths"
if ! cmp -s "$expected_paths" "$staged_paths"; then
	printf '%s\n' 'unexpected staged paths; refusing to commit:' >&2
	git diff --cached --name-only >&2
	exit 1
fi
git diff --cached --check
git commit -m 'feat(hatSql): add incremental distinct operator'
