#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mz021_replica_hot_handoff.go hat/hatSql/mz021_replica_hot_handoff_test.go hat/hatSql/mz021_replica_hot_handoff_benchmark_test.go
