#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/m052aa_auto_native_join.go hat/hatSql/m052aa_auto_native_join_test.go hat/hatSql/m052p_auto_native_dataflow.go
