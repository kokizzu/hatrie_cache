#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSchema/rolling_schema.go hat/hatSchema/rolling_schema_checkpoint.go hat/hatSchema/rolling_schema_checkpoint_test.go hat/hatSchema/rolling_schema_checkpoint_benchmark_test.go
