#!/bin/sh
set -eu

gofmt -w hat/hatSql/row_binary_stats_pruning.go hat/hatSql/row_binary_stats_pruning_test.go
