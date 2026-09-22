#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  T202_AUTOMATIC_LEADER_ELECTION.md \
  hat/hatTopology/election.go \
  hat/hatTopology/t202_automatic_leader_election_test.go \
  hat/hatTopology/t202_automatic_leader_election_benchmark_test.go \
  hat/hatCache/election.go \
  scripts/format-t202.sh \
  scripts/test-t202-automatic-leader-election.sh \
  scripts/benchmark-t202.sh \
  scripts/test-t202-package.sh \
  scripts/race-t202.sh \
  scripts/vet-t202.sh \
  scripts/stage-t202.sh \
  scripts/commit-t202.sh \
  scripts/push-t202.sh

git diff --cached --check
