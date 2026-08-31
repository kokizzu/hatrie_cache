#!/bin/sh
set -eu

gofmt -w hat/hatSql/projection_advisor.go hat/hatSql/projection_advisor_benchmark_test.go hat/hatSql/projection_advisor_test.go hat/hatSql/query.go
