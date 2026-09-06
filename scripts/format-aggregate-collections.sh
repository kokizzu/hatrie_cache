#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/aggregate_collections.go hat/hatSql/aggregate_collection_test.go hat/hatSql/query.go
