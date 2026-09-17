#!/bin/sh
set -eu
gofmt -w hat/hatSql/sql_sink_delivery_audit.go hat/hatSql/sql_sink_commit.go hat/hatSql/sql_sink_audit_test.go hat/hatSql/sql_sink_delivery_audit_benchmark_test.go
