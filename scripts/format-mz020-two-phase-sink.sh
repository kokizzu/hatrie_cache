#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/mz020_two_phase_sink_checkpoint.go \
  hat/hatSql/mz020_two_phase_sink_checkpoint_test.go \
  hat/hatSql/mz020_two_phase_sink_checkpoint_benchmark_test.go
