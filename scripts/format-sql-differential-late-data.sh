#!/usr/bin/env bash
set -euo pipefail

gofmt -w ./hat/hatSql/differential_late_data.go ./hat/hatSql/differential_late_data_test.go
