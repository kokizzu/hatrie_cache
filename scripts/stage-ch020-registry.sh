#!/usr/bin/env bash
set -euo pipefail

git add -- \
    ENGINE_IDEAS.md \
    CH020_ZERO_COPY_PART_SHARING.md \
    Makefile \
    hat/hatStorage/ch020_remote_part_registry.go \
    hat/hatStorage/ch020_remote_part_registry_test.go \
    hat/hatStorage/ch020_remote_part_registry_baseline_benchmark_test.go \
    scripts/benchmark-ch020-registry.sh \
    scripts/commit-ch020-registry.sh \
    scripts/format-ch020-registry.sh \
    scripts/push-ch020-registry.sh \
    scripts/race-ch020-registry.sh \
    scripts/stage-ch020-registry.sh \
    scripts/test-ch020-registry.sh \
    scripts/vet-ch020-registry.sh

git diff --cached --check
git diff --cached --stat
