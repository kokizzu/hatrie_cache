#!/usr/bin/env bash
set -euo pipefail

	gofmt -w \
	 hat/hatSql/differential_dataflow.go \
	 hat/hatSql/m_u46_differential_export.go \
	 hat/hatSql/m_u46_differential_export_test.go \
	 hat/hatSql/m_u46_differential_export_benchmark_test.go \
	 hat/hatSql/m_u46_differential_export_baseline_benchmark_test.go \
	 hat/hatSql/m_u46_differential_export_size_test.go
