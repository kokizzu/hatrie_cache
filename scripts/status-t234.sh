#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --stat
git diff --check
git diff -- api.go hat/hatCache/sql_transaction.go hat/hatCache/sql_transaction_options.go
git diff --no-index /dev/null hat/hatCache/t234_early_conflict_test.go || true
