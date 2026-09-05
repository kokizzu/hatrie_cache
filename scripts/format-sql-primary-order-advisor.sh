#!/bin/sh
set -eu
gofmt -w \
    hat/hatSql/index_advisor.go \
    hat/hatSql/index_advisor_primary_order_test.go \
    hat/hatSql/index_advisor_primary_order_benchmark_test.go
