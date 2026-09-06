#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/columnar_vertical_merge.go hat/hatSql/columnar_vertical_merge_test.go
