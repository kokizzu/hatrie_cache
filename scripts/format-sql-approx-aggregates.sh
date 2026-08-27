#!/bin/sh
set -eu

gofmt -w hat/hatSql/approx_aggregate.go hat/hatSql/approx_aggregate_test.go hat/hatSql/query.go
