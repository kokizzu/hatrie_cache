#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile README.md PRODUCT_IDEA_GAPS.md INSPIRATION_BACKLOG.md BENCHMARK.md \
  MU019_SOURCE_TRANSACTION_ENVELOPE.md \
  hat/hatSql/sql_source_ingestion.go \
  hat/hatSql/mu019_source_transaction_envelope.go \
  hat/hatSql/mu019_source_transaction_envelope_test.go \
  hat/hatSql/mu019_source_transaction_envelope_baseline_benchmark_test.go \
  scripts/format-mu019-source-transaction-envelope.sh \
  scripts/test-mu019-source-transaction-envelope.sh \
  scripts/test-mu019-legacy-source-contract.sh scripts/test-mu019-package.sh \
  scripts/race-mu019-source-transaction-envelope.sh \
  scripts/vet-mu019-source-transaction-envelope.sh \
  scripts/benchmark-mu019-source-transaction-envelope.sh \
  scripts/review-mu019-source-transaction-envelope.sh \
  scripts/stage-mu019-source-transaction-envelope.sh \
  scripts/commit-mu019-source-transaction-envelope.sh \
  scripts/push-mu019-source-transaction-envelope.sh
