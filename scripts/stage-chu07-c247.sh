#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to stage CH-U07 while unrelated index changes exist' >&2
	exit 1
fi

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CHU07_MUTATION_LIFECYCLE.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	hat/hatCache/async_command.go \
	hat/hatCache/chu07_mutation_lifecycle_benchmark_test.go \
	hat/hatCache/chu07_mutation_lifecycle_test.go \
	hat/hatCache/chu07_mutation_status.go \
	hat/hatCache/journal.go \
	hat/hatCache/system_tables.go \
	scripts/benchmark-chu07-c247.sh \
	scripts/format-chu07-c247.sh \
	scripts/memory-chu07-c247.sh \
	scripts/race-chu07-c247.sh \
	scripts/review-chu07-c247.sh \
	scripts/stage-chu07-c247.sh \
	scripts/commit-chu07-c247.sh \
	scripts/push-chu07-c247.sh \
	scripts/test-chu07-c247.sh \
	scripts/test-chu07-package-c247.sh \
	scripts/test-chu07-repo-c247.sh \
	scripts/verify-chu07-docs-c247.sh \
	scripts/vet-chu07-c247.sh

base_file="$(mktemp)"
staged_file="$(mktemp)"
makefile_patch="$(mktemp)"
cleanup() {
	rm -f "$base_file" "$staged_file" "$makefile_patch"
}
trap cleanup EXIT

git show HEAD:Makefile > "$base_file"
cp "$base_file" "$staged_file"
printf '\n%s\n' \
    '.PHONY: test-chu07-c247' \
    'test-chu07-c247:' \
    $'\t@bash ./scripts/test-chu07-c247.sh' \
    '' \
    '.PHONY: format-chu07-c247' \
    'format-chu07-c247:' \
    $'\t@bash ./scripts/format-chu07-c247.sh' \
    '' \
    '.PHONY: test-chu07-package-c247' \
    'test-chu07-package-c247:' \
    $'\t@bash ./scripts/test-chu07-package-c247.sh' \
    '' \
    '.PHONY: race-chu07-c247' \
    'race-chu07-c247:' \
    $'\t@bash ./scripts/race-chu07-c247.sh' \
    '' \
    '.PHONY: vet-chu07-c247' \
    'vet-chu07-c247:' \
    $'\t@bash ./scripts/vet-chu07-c247.sh' \
    '' \
    '.PHONY: benchmark-chu07-c247' \
    'benchmark-chu07-c247:' \
    $'\t@bash ./scripts/benchmark-chu07-c247.sh' \
    '' \
    '.PHONY: memory-chu07-c247' \
    'memory-chu07-c247:' \
    $'\t@bash ./scripts/memory-chu07-c247.sh' \
    '' \
    '.PHONY: verify-chu07-docs-c247' \
    'verify-chu07-docs-c247:' \
    $'\t@bash ./scripts/verify-chu07-docs-c247.sh' \
    '' \
    '.PHONY: test-chu07-repo-c247' \
    'test-chu07-repo-c247:' \
    $'\t@bash ./scripts/test-chu07-repo-c247.sh' \
    '' \
    '.PHONY: review-chu07-c247' \
    'review-chu07-c247:' \
    $'\t@bash ./scripts/review-chu07-c247.sh' \
    '' \
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
	printf '%s\n' 'Makefile synthetic patch unexpectedly empty' >&2
	exit 1
fi
sed -i "1s|^--- .*|--- a/Makefile|; 2s|^+++ .*|+++ b/Makefile|" "$makefile_patch"
git apply --cached "$makefile_patch"

git diff --cached --check
