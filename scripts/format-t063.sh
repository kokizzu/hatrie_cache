#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/replication.go hat/hatCache/replication_test.go hat/hatCache/replication_benchmark_test.go hat/hatCache/monitoring.go hat/hatReplication/model.go
