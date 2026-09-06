#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/aggregate_combinator.go \
	hat/hatSql/aggregate_combinator_test.go
