#!/bin/sh
set -eu

gofmt -w hat/hatSql/refresh_scheduler.go hat/hatSql/refresh_scheduler_budget_test.go hat/hatSql/refresh_scheduler_budget_benchmark_test.go
