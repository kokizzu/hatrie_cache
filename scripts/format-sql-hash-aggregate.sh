#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
gofmt -w hat/hatSql/hash_group_aggregate.go hat/hatSql/hash_group_aggregate_test.go hat/hatSql/query.go
