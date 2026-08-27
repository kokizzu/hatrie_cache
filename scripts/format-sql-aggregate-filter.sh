#!/bin/sh
set -eu

gofmt -w ./hat/hatSql/aggregate_filter.go ./hat/hatSql/aggregate_filter_test.go
