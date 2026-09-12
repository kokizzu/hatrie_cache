#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/debezium_changefeed.go \
	hat/hatSql/debezium_changefeed_test.go \
	hat/hatSql/debezium_changefeed_benchmark_test.go
