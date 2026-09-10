#!/usr/bin/env bash
set -euo pipefail

git add Makefile INSPIRATION.md README.md TOPOLOGY_CONSENSUS.md \
  hat/hatTopology/topology_commit.go \
  hat/hatCache/topology.go \
  hat/hatCache/topology_commit_test.go \
  hat/hatCache/topology_commit_benchmark_test.go \
  scripts/benchmark-c153.sh scripts/format-c153.sh scripts/race-c153.sh \
  scripts/review-c153.sh scripts/test-c153.sh scripts/vet-c153.sh \
  scripts/commit-c153.sh
git diff --cached --check
git commit -m "feat(topology): add consensus-bound metadata commits"
git push origin HEAD:master
