#!/usr/bin/env bash
set -euo pipefail

gofmt -w sql_source_frontier_api.go hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/sql_source_frontier_requirement.go hat/hatSql/mz007_frontier_requirement_test.go hat/hatSql/m066_mz007_frontier_requirement_baseline_benchmark_test.go
