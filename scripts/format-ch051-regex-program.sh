#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/regex.go hat/hatSql/query.go hat/hatSql/ch051_regex_program_test.go
