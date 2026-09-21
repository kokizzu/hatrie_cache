#!/usr/bin/env bash
set -euo pipefail

git commit --only -m 'hatDataStructure: add mutable packed R-tree overlay' -- \
  Makefile \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  TT021_MUTABLE_PACKED_RTREE.md \
  hat/hatDataStructure/tt021_packed_rtree.go \
  hat/hatDataStructure/vertical_ttl_delete_test.go \
  hat/hatDataStructure/spillable_arrangement_test.go \
  hat/hatDataStructure/tt021_mutable_packed_rtree.go \
  hat/hatDataStructure/tt021_mutable_packed_rtree_test.go \
  hat/hatDataStructure/tt021_mutable_packed_rtree_baseline_benchmark_test.go \
  hat/hatDataStructure/tt021_mutable_packed_rtree_benchmark_test.go \
  scripts/inspect-open-ideas.sh \
  scripts/format-tt021-mutable.sh \
  scripts/test-tt021-mutable.sh \
  scripts/test-tt021-package.sh \
  scripts/test-tt021-parent-package.sh \
  scripts/benchmark-tt021-mutable-before.sh \
  scripts/benchmark-tt021-mutable.sh \
  scripts/race-tt021-mutable.sh \
  scripts/vet-tt021-mutable.sh \
  scripts/review-tt021-mutable.sh \
  scripts/stage-tt021-mutable.sh \
  scripts/commit-tt021-mutable.sh \
  scripts/push-tt021-mutable.sh
