#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/sql_triggers.go hat/hatSql/tt029_before_trigger_baseline_benchmark_test.go hat/hatSql/tt029_before_trigger_test.go
