#!/bin/sh
set -eu

git add -A -- Makefile scripts/commit-repair-incomplete-sql-make-targets.sh
git diff --cached --check
git commit -m "fix: remove incomplete SQL make targets"
git push origin master
