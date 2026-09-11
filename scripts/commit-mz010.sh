#!/usr/bin/env bash
set -euo pipefail

git add Makefile ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md BENCHMARK.md MZ010_JOURNAL_SUBSCRIPTIONS.md hat/hatCache/journal.go hat/hatCache/journal_subscription.go hat/hatCache/journal_subscription_test.go hat/hatCache/journal_subscription_benchmark_test.go scripts/test-mz010-journal-subscription.sh scripts/test-race-mz010-journal-subscription.sh scripts/test-mz010-broad.sh scripts/benchmark-mz010-journal-subscription.sh scripts/format-mz010-journal-subscription.sh scripts/verify-mz010-docs.sh scripts/review-mz010.sh scripts/commit-mz010.sh scripts/push-mz010.sh scripts/status-mz010.sh
git diff --cached --check
git commit -m 'Add command journal subscriptions'
