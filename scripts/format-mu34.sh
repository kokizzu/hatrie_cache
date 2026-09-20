#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/journal_source_checkpoint.go \
  hat/hatCache/journal_historical_subscription.go \
  hat/hatCache/m_u34_historical_subscription_test.go \
  hat/hatCache/m_u34_historical_subscription_benchmark_test.go
