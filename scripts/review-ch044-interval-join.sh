#!/usr/bin/env bash
set -eu

git diff --check
git diff --cached --check
git status --short
git diff --stat
git diff --cached --stat
git diff -- hat/hatSql/m029_incremental_interval_join.go scripts/test-ch044-package.sh
