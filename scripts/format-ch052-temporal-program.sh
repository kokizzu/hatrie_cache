#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/time_zone.go hat/hatSql/query.go hat/hatSql/ch052_temporal_program_test.go
