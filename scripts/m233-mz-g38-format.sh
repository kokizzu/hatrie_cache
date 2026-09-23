#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPipeline/mz038_source_lag_alert.go hat/hatPipeline/mz038_source_lag_alert_test.go hat/hatPipeline/mz038_source_lag_alert_benchmark_test.go
