#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatHash/hash.go hat/hatSql/c212_hash_join.go hat/hatSql/c212_hash_join_test.go hat/hatSql/query.go hat/hatSql/join_order_stats.go
