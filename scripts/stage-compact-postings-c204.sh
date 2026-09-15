#!/usr/bin/env bash
set -euo pipefail

feature_makefile=$(mktemp)
working_makefile=$(mktemp)
cleanup() {
	cp "$working_makefile" Makefile
	rm -f "$feature_makefile" "$working_makefile"
}
trap cleanup EXIT

git show HEAD:Makefile > "$feature_makefile"
cat >> "$feature_makefile" <<'EOF'

.PHONY: test-compact-postings-c204
test-compact-postings-c204:
	bash ./scripts/test-compact-postings-c204.sh

.PHONY: benchmark-compact-postings-c204
benchmark-compact-postings-c204:
	bash ./scripts/benchmark-compact-postings-c204.sh

.PHONY: format-compact-postings-c204
format-compact-postings-c204:
	bash ./scripts/format-compact-postings-c204.sh

.PHONY: verify-compact-postings-c204
verify-compact-postings-c204:
	bash ./scripts/verify-compact-postings-c204.sh

.PHONY: test-compact-postings-package-c204
test-compact-postings-package-c204:
	bash ./scripts/test-compact-postings-package-c204.sh

.PHONY: inspect-compact-diff-c204
inspect-compact-diff-c204:
	bash ./scripts/inspect-compact-diff-c204.sh

.PHONY: stage-compact-postings-c204
stage-compact-postings-c204:
	bash ./scripts/stage-compact-postings-c204.sh

.PHONY: commit-compact-postings-c204
commit-compact-postings-c204:
	bash ./scripts/commit-compact-postings-c204.sh

.PHONY: push-compact-postings-c204
push-compact-postings-c204:
	bash ./scripts/push-compact-postings-c204.sh
EOF

cp Makefile "$working_makefile"
cp "$feature_makefile" Makefile
git add \
  Makefile \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  TR053_COMPACT_POSTING_LIST.md \
  hat/hatDataStructure/compact_posting.go \
  hat/hatDataStructure/compact_posting_test.go \
  hat/hatDataStructure/compact_posting_benchmark_test.go \
  hat/hatDataStructure/functional_index.go \
  hat/hatDataStructure/hash_index.go \
  hat/hatDataStructure/multikey_index.go \
  hat/hatDataStructure/conditional_index.go \
  scripts/test-compact-postings-c204.sh \
  scripts/benchmark-compact-postings-c204.sh \
  scripts/format-compact-postings-c204.sh \
  scripts/verify-compact-postings-c204.sh \
  scripts/test-compact-postings-package-c204.sh \
  scripts/inspect-compact-diff-c204.sh \
  scripts/stage-compact-postings-c204.sh \
  scripts/commit-compact-postings-c204.sh \
  scripts/push-compact-postings-c204.sh
