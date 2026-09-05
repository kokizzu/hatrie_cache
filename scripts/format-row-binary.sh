#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/row_binary.go hat/hatSql/row_binary_test.go
