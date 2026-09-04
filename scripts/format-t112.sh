#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/model.go \
  hat/hatReplication/metrics.go \
  hat/hatReplication/metrics_test.go \
  hat/hatCache/replication.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/replication_test.go
