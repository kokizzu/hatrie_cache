#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/json_path.go hat/hatSql/query.go hat/hatSql/ch030_json_path_program_test.go
