#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/asof_join.go \
	hat/hatSql/ch036_asof_sorted_fastpath_test.go
