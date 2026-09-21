#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
	 hat/hatSql/typed_table_mvcc.go \
	 hat/hatSql/m212_logical_compaction_test.go \
	 hat/hatSql/m212_logical_compaction_baseline_benchmark_test.go \
	 hat/hatSql/m212_logical_compaction_benchmark_test.go
