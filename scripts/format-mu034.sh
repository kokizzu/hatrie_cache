#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/subscription.go \
  hat/hatSql/differential_subscription.go \
  hat/hatSql/mu034_historical_subscription_checkpoint.go \
  hat/hatSql/mu034_historical_subscription_checkpoint_test.go \
  hat/hatSql/mu034_historical_subscription_checkpoint_benchmark_test.go
