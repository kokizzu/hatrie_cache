#!/bin/sh
set -eu

git add Makefile scripts/commit-sql-improvement-goal-verification.sh scripts/push-sql-improvement-goal-verification.sh scripts/verify-sql-improvement-goal.sh
git commit -m 'chore: add SQL improvement verification target'
