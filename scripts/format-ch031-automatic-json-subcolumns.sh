#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/ch031_automatic_json_subcolumn.go \
	hat/hatSql/ch031_automatic_json_subcolumn_test.go \
	hat/hatSql/ch031_automatic_json_subcolumn_benchmark_test.go
