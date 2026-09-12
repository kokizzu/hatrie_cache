#!/bin/sh
set -eu

git add Makefile WRITE_QUORUM.md INSPIRATION.md BENCHMARK.md \
  hat/hatReplication/write_quorum_fastpath.go \
  hat/hatReplication/write_quorum_fastpath_test.go \
  hat/hatReplication/write_quorum_fastpath_benchmark_test.go \
  scripts/test-write-quorum-fastpath.sh \
  scripts/benchmark-write-quorum-fastpath.sh \
  scripts/format-write-quorum-fastpath.sh \
  scripts/race-write-quorum-fastpath.sh \
  scripts/vet-write-quorum-fastpath.sh \
  scripts/review-write-quorum-fastpath.sh \
  scripts/commit-write-quorum-fastpath.sh \
  scripts/push-write-quorum-fastpath.sh
git diff --cached --check
git commit -m "feat: add opt-in quorum early acknowledgement"
