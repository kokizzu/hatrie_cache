#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat
git diff --cached --check
git diff --cached --stat
git diff --cached -- BENCHMARK.md PRODUCT_IDEA_GAPS.md README.md T042_RECOVERY_PARALLEL_REPLAY.md
