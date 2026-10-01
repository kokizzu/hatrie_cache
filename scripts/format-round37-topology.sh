#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatTopology/durable_membership.go \
  hat/hatTopology/tu13_durable_membership_test.go \
  hat/hatTopology/tu13_durable_membership_baseline_benchmark_test.go
