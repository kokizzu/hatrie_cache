#!/usr/bin/env bash
set -euo pipefail

exec gofmt -w \
  hat/hatCache/journal_checkpoint.go \
  hat/hatCache/journal_sink.go \
  hat/hatCache/journal_subscription.go \
  hat/hatCache/m_u34_subscription_checkpoint_test.go \
  hat/hatCache/m_u34_subscription_checkpoint_benchmark_test.go
