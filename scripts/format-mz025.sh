#!/usr/bin/env bash
set -eu

gofmt -w hat/hatSql/subscription.go hat/hatSql/mz025_tail_heartbeat_test.go hat/hatSql/mz025_tail_heartbeat_benchmark_test.go
