#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/projection_advisor.go \
	hat/hatSql/ch012_projection_advisor_forecast.go \
	hat/hatSql/ch012_projection_advisor_forecast_test.go \
	hat/hatSql/ch012_projection_advisor_forecast_benchmark_test.go
