#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatReplication/metrics.go \
    hat/hatReplication/t205_apply_metrics_benchmark_test.go \
    hat/hatReplication/t205_apply_metrics_test.go \
    hat/hatCache/monitoring.go \
    hat/hatCache/replication.go \
    hat/hatCache/t205_replication_apply_metrics_test.go
