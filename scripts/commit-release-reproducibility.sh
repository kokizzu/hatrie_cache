#!/bin/sh
set -eu

git add -- Makefile .github/workflows/release.yml ./scripts/verify-release-reproducibility.sh ./scripts/review-release-reproducibility.sh ./scripts/commit-release-reproducibility.sh
git commit -m "build: verify release reproducibility"
git push
