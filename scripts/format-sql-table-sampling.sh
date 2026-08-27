#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/table_sample.go hat/hatSql/table_sample_test.go
