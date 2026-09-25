#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/mz010_subscription_transport.go \
  hat/hatSql/mz010_subscription_transport_test.go \
  hat/hatSql/mz010_subscription_transport_benchmark_test.go
