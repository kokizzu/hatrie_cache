#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mz010_subscription_envelope.go hat/hatSql/mz010_subscription_envelope_test.go hat/hatSql/mz010_subscription_envelope_benchmark_test.go
