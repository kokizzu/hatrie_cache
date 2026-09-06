#!/usr/bin/env bash
set -euo pipefail

gofmt -w ./hat/hatSql/typed_table_monotone.go ./hat/hatSql/typed_table_monotone_test.go
