#!/usr/bin/env bash
set -eu

git add -- Makefile INSPIRATION.md BENCHMARK.md \
    scripts/review-m065j-rejected-small-distinct.sh \
    scripts/commit-m065j-rejected-small-distinct.sh \
    scripts/push-m065j-rejected-small-distinct.sh
git diff --cached --check
git commit -m "docs(benchmark): record rejected distinct fast path"
