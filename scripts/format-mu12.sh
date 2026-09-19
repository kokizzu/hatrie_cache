#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/explain_arrangement.go \
	hat/hatSql/m_u12_explain_arrangements_test.go \
	hat/hatSql/m_u12_explain_arrangements_benchmark_test.go
