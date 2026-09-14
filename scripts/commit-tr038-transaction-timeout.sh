#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  README.md \
  TR038_TRANSACTION_TIMEOUT.md \
  hat/hatCache/sql_transaction.go \
  hat/hatCache/sql_transaction_options.go \
  hat/hatCache/tr038_transaction_timeout_benchmark_test.go \
  hat/hatCache/tr038_transaction_timeout_test.go \
  scripts/benchmark-tr038-transaction-timeout.sh \
  scripts/commit-tr038-transaction-timeout.sh \
  scripts/format-tr038-transaction-timeout.sh \
  scripts/push-tr038-transaction-timeout.sh \
  scripts/race-tr038-transaction-timeout.sh \
  scripts/review-tr038-transaction-timeout.sh \
  scripts/test-tr038-transaction-timeout.sh \
  scripts/vet-tr038-transaction-timeout.sh
git diff --cached --check
git commit -m "Add opt-in SQL transaction timeouts"
git status --short
