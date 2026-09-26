#!/bin/sh
set -eu
gofmt -w hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/catalog.go hat/hatSql/session.go hat/hatSql/m052p_auto_native_dataflow.go hat/hatSql/m090c_projected_source.go hat/hatSql/m090c_projected_source_test.go
