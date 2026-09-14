#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/query.go hat/hatSql/in_program.go hat/hatSql/ch053_in_program_test.go
