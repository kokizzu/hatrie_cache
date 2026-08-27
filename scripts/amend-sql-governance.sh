#!/bin/sh
set -eu

git add Makefile scripts/amend-sql-governance.sh scripts/commit-sql-governance.sh scripts/push-sql-governance.sh
git commit --amend --no-edit
