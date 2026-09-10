#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  scripts/commit-t042-rejection.sh \
  scripts/push-t042-rejection.sh
git diff --cached --check
git commit -m "docs(benchmark): record rejected parallel replay"
