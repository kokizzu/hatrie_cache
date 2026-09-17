#!/bin/sh
set -eu
gofmt -w hat/hatSql/projection_advisor.go hat/hatSql/ch017_projection_advisor_test.go hat/hatSql/ch017_projection_advisor_benchmark_test.go
