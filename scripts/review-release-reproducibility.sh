#!/bin/sh
set -eu

git diff --check -- Makefile .github/workflows/release.yml ./scripts/verify-release-reproducibility.sh ./scripts/review-release-reproducibility.sh ./scripts/commit-release-reproducibility.sh
git status --short
