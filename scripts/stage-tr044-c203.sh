#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  TR044_COMPACT_PEER_COMPRESSION.md \
  hat/hatPeer/compact_compression.go \
  hat/hatPeer/compact_compression_test.go \
  hat/hatPeer/compact_compression_benchmark_test.go \
  scripts/test-tr044-c203.sh \
  scripts/format-tr044-c203.sh \
  scripts/benchmark-tr044-c203.sh \
  scripts/verify-tr044-c203.sh \
  scripts/stage-tr044-c203.sh \
  scripts/inspect-staged-tr044-c203.sh \
  scripts/commit-tr044-c203.sh \
  scripts/push-tr044-c203.sh

makefile_stage=$(mktemp)
trap 'rm -f "$makefile_stage"' EXIT
git show HEAD:Makefile > "$makefile_stage"
cat >> "$makefile_stage" <<'EOF'

.PHONY: test-tr044-c203
test-tr044-c203:
	bash ./scripts/test-tr044-c203.sh

.PHONY: format-tr044-c203
format-tr044-c203:
	bash ./scripts/format-tr044-c203.sh

.PHONY: benchmark-tr044-c203
benchmark-tr044-c203:
	bash ./scripts/benchmark-tr044-c203.sh

.PHONY: verify-tr044-c203
verify-tr044-c203:
	bash ./scripts/verify-tr044-c203.sh

.PHONY: stage-tr044-c203
stage-tr044-c203:
	bash ./scripts/stage-tr044-c203.sh

.PHONY: inspect-staged-tr044-c203
inspect-staged-tr044-c203:
	bash ./scripts/inspect-staged-tr044-c203.sh

.PHONY: commit-tr044-c203
commit-tr044-c203:
	bash ./scripts/commit-tr044-c203.sh

.PHONY: push-tr044-c203
push-tr044-c203:
	bash ./scripts/push-tr044-c203.sh
EOF
makefile_blob=$(git hash-object -w "$makefile_stage")
git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile
