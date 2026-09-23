#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md INSPIRATION_ROUND2.md T206_DETERMINISTIC_REPLICA_BOOTSTRAP.md \
    hat/hatReplication/tu206_deterministic_replica_bootstrap.go \
    hat/hatReplication/tu206_deterministic_replica_bootstrap_test.go \
    hat/hatReplication/tu206_deterministic_replica_bootstrap_benchmark_test.go \
    scripts/format-t206.sh scripts/test-t206.sh scripts/test-t206-package.sh \
    scripts/race-t206.sh scripts/vet-t206.sh scripts/benchmark-t206-before.sh \
    scripts/benchmark-t206.sh scripts/stage-t206.sh scripts/commit-t206.sh \
    scripts/push-t206.sh
git diff --cached --check
git diff --cached --name-only
