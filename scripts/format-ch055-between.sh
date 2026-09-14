#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/between_program.go hat/hatSql/query.go hat/hatSql/ch055_between_program_test.go
