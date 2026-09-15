#!/usr/bin/env bash
set -euo pipefail

for path in \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	PRODUCT_IDEA_GAPS.md \
	hat/hatCache/count_min_sketch.go \
	hat/hatCache/count_min_sketch_merge_import_test.go \
	hat/hatCache/count_min_sketch_merge_test.go \
	scripts/check-countmin-merge-c223.sh \
	scripts/format-countmin-merge-c223.sh \
	scripts/run-countmin-merge-isolated-c223.sh \
	scripts/stage-countmin-merge-c223.sh \
	scripts/test-countmin-merge-c223.sh \
	scripts/commit-countmin-merge-c223.sh \
	scripts/push-countmin-merge-c223.sh; do
	if git diff --cached --quiet -- "$path"; then
		:
	else
		printf 'pre-existing staged changes in %s\n' "$path" >&2
		exit 1
	fi
done

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	PRODUCT_IDEA_GAPS.md \
	hat/hatCache/count_min_sketch.go \
	hat/hatCache/count_min_sketch_merge_import_test.go \
	hat/hatCache/count_min_sketch_merge_test.go \
	scripts/check-countmin-merge-c223.sh \
	scripts/format-countmin-merge-c223.sh \
	scripts/run-countmin-merge-isolated-c223.sh \
	scripts/stage-countmin-merge-c223.sh \
	scripts/test-countmin-merge-c223.sh \
	scripts/commit-countmin-merge-c223.sh \
	scripts/push-countmin-merge-c223.sh

makefile=$(mktemp)
trap 'rm -f "$makefile"' EXIT
git show HEAD:Makefile > "$makefile"
cat >> "$makefile" <<'EOF'

.PHONY: test-countmin-merge-c223
test-countmin-merge-c223:
	@bash ./scripts/test-countmin-merge-c223.sh

.PHONY: format-countmin-merge-c223
format-countmin-merge-c223:
	@bash ./scripts/format-countmin-merge-c223.sh

.PHONY: test-countmin-package-c223
test-countmin-package-c223:
	@bash ./scripts/run-countmin-merge-isolated-c223.sh package

.PHONY: race-countmin-merge-c223
race-countmin-merge-c223:
	@bash ./scripts/run-countmin-merge-isolated-c223.sh race

.PHONY: vet-countmin-merge-c223
vet-countmin-merge-c223:
	@bash ./scripts/run-countmin-merge-isolated-c223.sh vet

.PHONY: benchmark-countmin-merge-c223
benchmark-countmin-merge-c223:
	@bash ./scripts/run-countmin-merge-isolated-c223.sh benchmark

.PHONY: check-countmin-merge-c223
check-countmin-merge-c223:
	@bash ./scripts/check-countmin-merge-c223.sh

.PHONY: stage-countmin-merge-c223
stage-countmin-merge-c223:
	@bash ./scripts/stage-countmin-merge-c223.sh

.PHONY: commit-countmin-merge-c223
commit-countmin-merge-c223:
	@bash ./scripts/commit-countmin-merge-c223.sh

.PHONY: push-countmin-merge-c223
push-countmin-merge-c223:
	@bash ./scripts/push-countmin-merge-c223.sh
EOF

blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo 100644 "$blob" Makefile
git diff --cached --check
git diff --cached --stat
