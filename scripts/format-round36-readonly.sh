#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/read_only_gate.go \
  hat/hatReplication/read_only_gate_test.go \
  hat/hatReplication/read_only_gate_benchmark_test.go
