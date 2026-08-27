#!/bin/sh
set -eu

gofmt -w hat/hatSql/sql_telemetry.go hat/hatSql/sql_telemetry_test.go
