#!/bin/sh
set -eu

git add -A -- Makefile hat/hatCache/local_verification_test.go scripts/test-local-verification.sh scripts/commit-github-ci-disabled-policy.sh
git diff --cached --check
git commit -m "test: align local verification with disabled CI"
git push origin master
