#!/bin/sh
set -eu

git diff --check -- Makefile go.mod .github/workflows/release.yml ./scripts/verify-vulnerabilities.sh ./scripts/review-toolchain-security.sh ./scripts/commit-toolchain-security.sh
git status --short
