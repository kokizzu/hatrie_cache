#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ009_TIMESTAMP_DOMAIN_LEASE.md \
  Makefile \
  README.md \
  hat/hatPipeline/mz009_timestamp_domain_lease.go \
  hat/hatPipeline/mz009_timestamp_domain_lease_benchmark_test.go \
  hat/hatPipeline/mz009_timestamp_domain_lease_test.go \
  scripts/benchmark-mz009-timestamp-domain-lease.sh \
  scripts/commit-mz009.sh \
  scripts/format-mz009-timestamp-domain-lease.sh \
  scripts/push-mz009.sh \
  scripts/race-mz009-timestamp-domain-lease.sh \
  scripts/review-mz009.sh \
  scripts/stage-mz009.sh \
  scripts/test-mz009-package.sh \
  scripts/test-mz009-timestamp-domain-lease.sh \
  scripts/vet-mz009-timestamp-domain-lease.sh

git diff --cached --check
git diff --cached --stat
git status --short
