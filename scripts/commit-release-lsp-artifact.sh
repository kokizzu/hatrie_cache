#!/bin/sh
set -eu

git add -- .gitignore Makefile .github/workflows/release.yml ./scripts/release-build.sh ./scripts/review-release-lsp-artifact.sh ./scripts/commit-release-lsp-artifact.sh
git commit -m "build: release SQL language server binary"
git push
