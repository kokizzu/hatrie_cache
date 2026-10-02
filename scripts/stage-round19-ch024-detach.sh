#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CH024_DETACH_ATTACH.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatStorage/ch024_detach_benchmark_test.go \
  hat/hatStorage/ch024_detach_test.go \
  hat/hatStorage/remote_part_attachment.go \
  scripts/commit-round19-ch024-detach.sh \
  scripts/push-round19-ch024-detach.sh \
  scripts/run-ch024-detach-checks.sh \
  scripts/stage-round19-ch024-detach.sh

git diff --cached --check
git status --short
