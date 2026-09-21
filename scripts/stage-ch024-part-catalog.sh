#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  CH024_DETACH_ATTACH_PARTS.md \
  hat/hatMerkle/part_catalog.go \
  hat/hatMerkle/part_transfer.go \
  hat/hatMerkle/ch024_part_catalog_test.go \
  hat/hatMerkle/ch024_part_catalog_benchmark_test.go \
  scripts/format-ch024-part-catalog.sh \
  scripts/test-ch024-part-catalog.sh \
  scripts/test-ch024-package.sh \
  scripts/benchmark-ch024-part-catalog.sh \
  scripts/race-ch024-part-catalog.sh \
  scripts/stage-ch024-part-catalog.sh \
  scripts/commit-ch024-part-catalog.sh \
  scripts/push-ch024-part-catalog.sh
