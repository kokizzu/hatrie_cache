#!/usr/bin/env bash
set -euo pipefail

expected_file="$(mktemp)"
actual_file="$(mktemp)"
base_file="$(mktemp)"
staged_file="$(mktemp)"
makefile_patch="$(mktemp)"
cleanup() {
	rm -f "$expected_file" "$actual_file" "$base_file" "$staged_file" "$makefile_patch"
}
trap cleanup EXIT

git add -- scripts/stage-chu07-c247.sh scripts/commit-chu07-c247.sh scripts/push-chu07-c247.sh
git show :Makefile > "$base_file"
cp "$base_file" "$staged_file"
printf '\n%s\n' \
	'.PHONY: stage-chu07-c247' \
	'stage-chu07-c247:' \
	$'\t@bash ./scripts/stage-chu07-c247.sh' \
	'' \
	'.PHONY: commit-chu07-c247' \
	'commit-chu07-c247:' \
	$'\t@bash ./scripts/commit-chu07-c247.sh' \
	'' \
	'.PHONY: push-chu07-c247' \
	'push-chu07-c247:' \
	$'\t@bash ./scripts/push-chu07-c247.sh' >> "$staged_file"
if diff -u "$base_file" "$staged_file" > "$makefile_patch"; then
	printf '%s\n' 'Makefile delivery-target patch unexpectedly empty' >&2
	exit 1
fi
sed -i "1s|^--- .*|--- a/Makefile|; 2s|^+++ .*|+++ b/Makefile|" "$makefile_patch"
git apply --cached "$makefile_patch"

printf '%s\n' \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CHU07_MUTATION_LIFECYCLE.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	hat/hatCache/async_command.go \
	hat/hatCache/chu07_mutation_lifecycle_benchmark_test.go \
	hat/hatCache/chu07_mutation_lifecycle_test.go \
	hat/hatCache/chu07_mutation_status.go \
	hat/hatCache/journal.go \
	hat/hatCache/system_tables.go \
	scripts/benchmark-chu07-c247.sh \
	scripts/commit-chu07-c247.sh \
	scripts/format-chu07-c247.sh \
	scripts/memory-chu07-c247.sh \
	scripts/push-chu07-c247.sh \
	scripts/race-chu07-c247.sh \
	scripts/review-chu07-c247.sh \
	scripts/stage-chu07-c247.sh \
	scripts/test-chu07-c247.sh \
	scripts/test-chu07-package-c247.sh \
	scripts/test-chu07-repo-c247.sh \
	scripts/verify-chu07-docs-c247.sh \
	scripts/vet-chu07-c247.sh | sort > "$expected_file"
git diff --cached --name-only | sort > "$actual_file"
diff -u "$expected_file" "$actual_file"
git diff --cached --check
git commit -m 'feat: expose mutation lifecycle status [skip ci]'
