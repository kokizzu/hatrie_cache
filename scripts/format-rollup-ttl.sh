#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/rollup.go hat/hatSql/rollup_ttl_test.go
