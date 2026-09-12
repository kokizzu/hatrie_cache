#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/differential_subscription.go \
  hat/hatSql/differential_subscription_benchmark_test.go \
  hat/hatSql/differential_subscription_test.go \
  hat/hatSql/differential_subscription_public_test.go \
  hat/hatSql/subscription.go
