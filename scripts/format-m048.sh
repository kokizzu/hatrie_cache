#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m048_dataflow_plan_codec.go \
	hat/hatSql/m048_dataflow_plan_codec_test.go \
	hat/hatSql/m048_dataflow_plan_codec_benchmark_test.go
