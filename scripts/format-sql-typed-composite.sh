#!/bin/sh
set -eu

gofmt -w hat/hatCache/main.go hat/hatCache/monitoring.go hat/hatCache/sql_query.go hat/hatCache/sql_typed_composite_test.go hat/hatCache/sql_typed_composite_benchmark_test.go hat/hatCache/sql_composite_range_borrowed_test.go hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/execution_single_source_envelope_test.go hat/hatSql/execution_single_source_envelope_benchmark_test.go hat/hatSql/metrics_byte_accounting_test.go hat/hatSql/metrics_byte_accounting_benchmark_test.go hat/hatSql/source_borrowed_test.go
