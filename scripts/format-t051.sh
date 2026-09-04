#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/model.go \
  hat/hatCache/replication.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/replication_lag_test.go
