#!/usr/bin/env bash
set -euo pipefail
git add BENCHMARK.md INSPIRATION_BACKLOG.md Makefile README.md TR036_READ_ONLY_TRANSACTIONS.md hat/hatCache/sql_transaction.go hat/hatCache/sql_transaction_options.go hat/hatCache/tr036_read_only_transaction_test.go scripts/benchmark-tr036-read-only-transaction.sh scripts/commit-tr036-read-only-transaction.sh scripts/format-tr036-read-only-transaction.sh scripts/push-tr036-read-only-transaction.sh scripts/race-tr036-read-only-transaction.sh scripts/test-tr036-read-only-transaction.sh scripts/vet-tr036-read-only-transaction.sh
git commit -m "feat: add read-only SQL transactions"
