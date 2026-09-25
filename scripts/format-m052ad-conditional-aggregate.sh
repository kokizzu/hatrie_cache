#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m052c_native_dataflow.go \
	hat/hatSql/query.go \
	hat/hatSql/m052ad_auto_native_conditional_aggregate_test.go
