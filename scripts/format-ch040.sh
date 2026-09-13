#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/tdigest.go hat/hatDataStructure/tdigest_test.go hat/hatSql/approx_aggregate.go hat/hatSql/tdigest_aggregate_test.go hat/hatSql/query.go
