#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/columnar_case_projection.go hat/hatSql/chu63_case_projection_benchmark_test.go hat/hatSql/chu63_case_projection_test.go
