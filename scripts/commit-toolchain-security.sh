#!/bin/sh
set -eu

git add -- Makefile go.mod .github/workflows/release.yml ./scripts/verify-vulnerabilities.sh ./scripts/review-toolchain-security.sh ./scripts/commit-toolchain-security.sh
git commit -m "build: update Go security toolchain"
git push
