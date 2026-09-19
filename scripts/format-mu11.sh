#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/typed_table_arrangement_advisor.go \
	hat/hatSql/m_u11_arrangement_advisor_test.go \
	hat/hatSql/m_u11_arrangement_advisor_benchmark_test.go
