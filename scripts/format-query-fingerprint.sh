#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query_fingerprint.go hat/hatSql/query_fingerprint_test.go
