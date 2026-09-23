#!/usr/bin/env bash
set -euo pipefail

git add Makefile INSPIRATION_ROUND2.md scripts/t244-245-test.sh scripts/t244-245-benchmark.sh \
  scripts/stage-t244-245.sh scripts/commit-t244-245.sh scripts/push-t244-245.sh
git diff --cached --check
git diff --cached --stat
git status --short
