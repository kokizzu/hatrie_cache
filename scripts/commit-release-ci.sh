#!/bin/sh
set -eu

git add Makefile .github/workflows/ci.yml .github/workflows/release.yml cmd/hatrie-sbom/main.go hat/hatCache/local_verification_test.go scripts/commit-release-ci.sh scripts/format-release-tooling.sh scripts/push-release-ci.sh scripts/release-build.sh scripts/release-sbom.sh scripts/verify-long-running.sh scripts/verify-vulnerabilities.sh
git commit -m 'feat: add reproducible release and CI automation'
