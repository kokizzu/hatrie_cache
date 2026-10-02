#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  CH019_REMOTE_PART_CHECKS.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatStorage/ch019_remote_part_checksum_benchmark_test.go \
  hat/hatStorage/ch019_remote_part_checksum_test.go \
  hat/hatStorage/remote_part_cache.go \
  hat/hatStorage/remote_part_checksum.go \
  scripts/run-ch019-remote-part-checks.sh \
  scripts/stage-ch019-remote-part-checks.sh \
  scripts/commit-ch019-remote-part-checks.sh \
  scripts/push-ch019-remote-part-checks.sh
git diff --cached --check
