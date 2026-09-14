#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/like_program.go hat/hatSql/query.go hat/hatSql/ch056_like_program_test.go
