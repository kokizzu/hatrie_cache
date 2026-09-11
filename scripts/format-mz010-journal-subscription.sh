#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/journal_subscription.go \
  hat/hatCache/journal_subscription_test.go \
  hat/hatCache/journal_subscription_benchmark_test.go
