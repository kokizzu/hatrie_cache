#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/journal_subscription.go \
  hat/hatCache/m204_bounded_subscription_benchmark_test.go \
  hat/hatCache/m204_bounded_subscription_test.go
