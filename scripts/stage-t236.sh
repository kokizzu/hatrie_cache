#!/usr/bin/env bash
set -euo pipefail

git add Makefile BENCHMARK.md INSPIRATION_ROUND2.md hat/hatPipeline/t236_mailbox_benchmark_test.go scripts/benchmark-t236.sh scripts/test-t236-rejected.sh scripts/verify-t236-rejection.sh scripts/format-t236-rejection.sh scripts/stage-t236.sh scripts/commit-t236.sh scripts/push-t236.sh scripts/status-t236.sh
git diff --cached --check
git diff --cached --stat
