#!/bin/sh
set -eu

git add \
	Makefile \
	scripts/amend-tt046-memory-accounting.sh \
	scripts/commit-tt046-memory-accounting.sh \
	scripts/push-tt046-memory-accounting.sh
git diff --cached --check
git commit --amend --no-edit
