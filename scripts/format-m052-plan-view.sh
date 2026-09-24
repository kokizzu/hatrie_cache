#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m052_reusable_dataflow_plan_view.go hat/hatSql/m052_reusable_dataflow_plan_view_test.go hat/hatSql/m052_reusable_dataflow_plan_view_benchmark_test.go
