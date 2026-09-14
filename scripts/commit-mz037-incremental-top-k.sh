#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	ENGINE_IDEAS.md
	MZ037_INCREMENTAL_TOP_K.md
	README.md
	hat/hatSql/c213_incremental_top_k.go
	hat/hatSql/c213_incremental_top_k_benchmark_test.go
	hat/hatSql/c213_incremental_top_k_test.go
	scripts/benchmark-mz037-incremental-top-k.sh
	scripts/commit-mz037-incremental-top-k.sh
	scripts/format-mz037-incremental-top-k.sh
	scripts/push-mz037-incremental-top-k.sh
	scripts/race-mz037-incremental-top-k.sh
	scripts/review-mz037-incremental-top-k.sh
	scripts/test-mz037-incremental-top-k.sh
	scripts/verify-mz037-incremental-top-k.sh
	scripts/vet-mz037-incremental-top-k.sh
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
printf '\n%s\n' '.PHONY: test-mz037-incremental-top-k benchmark-mz037-incremental-top-k format-mz037-incremental-top-k race-mz037-incremental-top-k vet-mz037-incremental-top-k review-mz037-incremental-top-k verify-mz037-incremental-top-k' >> "$feature_makefile"
printf '%s\n' 'test-mz037-incremental-top-k:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/test-mz037-incremental-top-k.sh' >> "$feature_makefile"
printf '%s\n' 'benchmark-mz037-incremental-top-k:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/benchmark-mz037-incremental-top-k.sh' >> "$feature_makefile"
printf '%s\n' 'format-mz037-incremental-top-k:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/format-mz037-incremental-top-k.sh' >> "$feature_makefile"
printf '%s\n' 'race-mz037-incremental-top-k:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/race-mz037-incremental-top-k.sh' >> "$feature_makefile"
printf '%s\n' 'vet-mz037-incremental-top-k:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/vet-mz037-incremental-top-k.sh' >> "$feature_makefile"
printf '%s\n' 'review-mz037-incremental-top-k:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/review-mz037-incremental-top-k.sh' >> "$feature_makefile"
printf '%s\n' 'verify-mz037-incremental-top-k:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/verify-mz037-incremental-top-k.sh' >> "$feature_makefile"
printf '%s\n' 'commit-mz037-incremental-top-k:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/commit-mz037-incremental-top-k.sh' >> "$feature_makefile"
printf '%s\n' 'push-mz037-incremental-top-k:' >> "$feature_makefile"
printf '%s\n' $'\t@bash scripts/push-mz037-incremental-top-k.sh' >> "$feature_makefile"

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
git commit -m 'feat(hatSql): add incremental weighted top-k'
