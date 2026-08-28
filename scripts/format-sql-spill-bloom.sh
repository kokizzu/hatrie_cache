#!/bin/sh
set -eu

gofmt -w ./hat/hatSql/query.go ./hat/hatSql/spill_bloom_test.go
