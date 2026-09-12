#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/subscription.go \
  hat/hatSql/differential_subscription.go \
  hat/hatSql/differential_subscription_order_test.go \
  hat/hatSql/differential_subscription_order_benchmark_test.go
