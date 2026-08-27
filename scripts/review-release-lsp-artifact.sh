#!/bin/sh
set -eu

git diff --check -- .gitignore Makefile .github/workflows/release.yml ./scripts/release-build.sh ./scripts/review-release-lsp-artifact.sh ./scripts/commit-release-lsp-artifact.sh
git status --short
