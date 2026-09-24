#!/usr/bin/env bash
set -euo pipefail

	gofmt -w \
	 hat/hatSchema/materialized.go \
	 hat/hatSchema/tt027_generated_columns.go \
	 hat/hatSchema/tt027_generated_columns_test.go \
	 hat/hatSchema/tt027_generated_columns_benchmark_test.go
