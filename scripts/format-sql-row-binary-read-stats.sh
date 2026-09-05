#!/bin/sh
set -eu

gofmt -w hat/hatSql/row_binary_read_stats.go hat/hatSql/row_binary_read_stats_test.go
