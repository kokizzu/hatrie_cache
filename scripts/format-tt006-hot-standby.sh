#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatReplication/tt006_hot_standby.go \
  hat/hatReplication/tt006_hot_standby_test.go \
  hat/hatReplication/tt006_hot_standby_benchmark_test.go
