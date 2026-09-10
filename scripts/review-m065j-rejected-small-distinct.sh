#!/usr/bin/env bash
set -eu

git diff --check
git status --short
git diff --stat -- Makefile INSPIRATION.md BENCHMARK.md \
    scripts/review-m065j-rejected-small-distinct.sh \
    scripts/commit-m065j-rejected-small-distinct.sh \
    scripts/push-m065j-rejected-small-distinct.sh
