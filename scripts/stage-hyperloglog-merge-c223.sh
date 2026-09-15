#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet -- BENCHMARK.md; then printf 'pre-existing staged changes in BENCHMARK.md\n' >&2; exit 1; fi
if ! git diff --cached --quiet -- INSPIRATION_ROUND2.md; then printf 'pre-existing staged changes in INSPIRATION_ROUND2.md\n' >&2; exit 1; fi
if ! git diff --cached --quiet -- PRODUCT_IDEA_GAPS.md; then printf 'pre-existing staged changes in PRODUCT_IDEA_GAPS.md\n' >&2; exit 1; fi
if ! git diff --cached --quiet -- Makefile; then printf 'pre-existing staged changes in Makefile\n' >&2; exit 1; fi
if ! git diff --cached --quiet -- hat/hatDataStructure/hyperloglog.go; then printf 'pre-existing staged changes in hyperloglog.go\n' >&2; exit 1; fi
if ! git diff --cached --quiet -- hat/hatDataStructure/hyperloglog_merge_test.go; then printf 'pre-existing staged changes in hyperloglog_merge_test.go\n' >&2; exit 1; fi
if ! git diff --cached --quiet -- hat/hatDataStructure/hyperloglog_merge_benchmark_test.go; then printf 'pre-existing staged changes in hyperloglog_merge_benchmark_test.go\n' >&2; exit 1; fi

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT
git show HEAD:Makefile > "$base_makefile"
cat <<'MAKE_TARGETS' >> "$base_makefile"

.PHONY: test-hyperloglog-merge-c223
test-hyperloglog-merge-c223:
	@bash ./scripts/test-hyperloglog-merge-c223.sh

.PHONY: format-hyperloglog-merge-c223
format-hyperloglog-merge-c223:
	@bash ./scripts/format-hyperloglog-merge-c223.sh

.PHONY: benchmark-hyperloglog-merge-c223
benchmark-hyperloglog-merge-c223:
	@bash ./scripts/benchmark-hyperloglog-merge-c223.sh

.PHONY: verify-hyperloglog-merge-c223
verify-hyperloglog-merge-c223:
	@bash ./scripts/verify-hyperloglog-merge-c223.sh

.PHONY: stage-hyperloglog-merge-c223
stage-hyperloglog-merge-c223:
	@bash ./scripts/stage-hyperloglog-merge-c223.sh

.PHONY: commit-hyperloglog-merge-c223
commit-hyperloglog-merge-c223:
	@bash ./scripts/commit-hyperloglog-merge-c223.sh

.PHONY: push-hyperloglog-merge-c223
push-hyperloglog-merge-c223:
	@bash ./scripts/push-hyperloglog-merge-c223.sh
MAKE_TARGETS

git add -- BENCHMARK.md INSPIRATION_ROUND2.md PRODUCT_IDEA_GAPS.md
git add -- \
  hat/hatDataStructure/hyperloglog.go \
  hat/hatDataStructure/hyperloglog_merge_test.go \
  hat/hatDataStructure/hyperloglog_merge_benchmark_test.go
git add -- \
  scripts/benchmark-hyperloglog-merge-c223.sh \
  scripts/commit-hyperloglog-merge-c223.sh \
  scripts/format-hyperloglog-merge-c223.sh \
  scripts/push-hyperloglog-merge-c223.sh \
  scripts/stage-hyperloglog-merge-c223.sh \
  scripts/test-hyperloglog-merge-c223.sh \
  scripts/verify-hyperloglog-merge-c223.sh
makefile_blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --name-only
