#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	echo "refusing to stage: the index already contains changes" >&2
	exit 1
fi

paths=(
	BENCHMARK.md
	INSPIRATION_BACKLOG.md
	Makefile
	README.md
	TR052_ORDERED_INDEX_SMALL_VECTOR.md
	hat/hatDataStructure/ordered_index.go
	hat/hatDataStructure/ordered_index_benchmark_test.go
	hat/hatDataStructure/ordered_index_test.go
	scripts/benchmark-ordered-index-c203.sh
	scripts/commit-ordered-index-c203.sh
	scripts/inspect-staged-ordered-index-c203.sh
	scripts/push-ordered-index-c203.sh
	scripts/stage-ordered-index-c203.sh
	scripts/test-ordered-index-c203.sh
	scripts/verify-ordered-index-c203.sh
)

makefile_backup=$(mktemp)
makefile_staged=$(mktemp)
restore_makefile() {
	mv "$makefile_backup" Makefile
	rm -f "$makefile_staged"
}
trap restore_makefile EXIT

cp Makefile "$makefile_backup"
git show HEAD:Makefile >"$makefile_staged"
cat >>"$makefile_staged" <<'MAKEFILE_TARGETS'

.PHONY: test-ordered-index-c203
test-ordered-index-c203:
	bash ./scripts/test-ordered-index-c203.sh

.PHONY: benchmark-ordered-index-c203
benchmark-ordered-index-c203:
	bash ./scripts/benchmark-ordered-index-c203.sh

.PHONY: verify-ordered-index-c203
verify-ordered-index-c203:
	bash ./scripts/verify-ordered-index-c203.sh

.PHONY: stage-ordered-index-c203
stage-ordered-index-c203:
	bash ./scripts/stage-ordered-index-c203.sh

.PHONY: inspect-staged-ordered-index-c203
inspect-staged-ordered-index-c203:
	bash ./scripts/inspect-staged-ordered-index-c203.sh

.PHONY: commit-ordered-index-c203
commit-ordered-index-c203:
	bash ./scripts/commit-ordered-index-c203.sh

.PHONY: push-ordered-index-c203
push-ordered-index-c203:
	bash ./scripts/push-ordered-index-c203.sh
MAKEFILE_TARGETS
mv "$makefile_staged" Makefile

git add -- "${paths[@]}"
if ! git diff --cached --check; then
	echo "staged patch contains whitespace errors" >&2
	exit 1
fi
git diff --cached --stat
