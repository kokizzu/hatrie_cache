#!/usr/bin/env bash
set -euo pipefail

git add -- \
	Makefile \
	README.md \
	scripts/cleanup-go-build-preview.sh \
	scripts/cleanup-go-build-tmp.sh \
	scripts/cleanup-go-build.sh \
	scripts/commit-cleanup-go-build.sh \
	scripts/push-cleanup-go-build.sh \
	scripts/review-cleanup-go-build.sh \
	scripts/stage-cleanup-go-build.sh \
	scripts/test-cleanup-go-build.sh
git diff --cached --check
