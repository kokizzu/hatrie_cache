#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatPipeline/mz032_late_data_reclock.go \
    hat/hatPipeline/mz032_late_data_reclock_test.go \
    hat/hatPipeline/mz032_late_data_reclock_benchmark_test.go
