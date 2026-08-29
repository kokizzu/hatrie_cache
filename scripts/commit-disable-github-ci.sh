#!/bin/sh
set -eu

git add -A -- Makefile scripts/verify-github-ci-disabled.sh scripts/commit-disable-github-ci.sh
git diff --cached --check
git commit -m "chore: disable GitHub push CI"
git push origin master
