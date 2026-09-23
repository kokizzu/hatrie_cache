#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/differential_subscription.go hat/hatSql/debezium_changefeed.go hat/hatSql/m213_differential_consolidation_test.go hat/hatSql/m213_differential_consolidation_benchmark_test.go
