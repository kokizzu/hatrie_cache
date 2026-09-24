#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/columnar_json_subcolumn_scan.go hat/hatSql/m047_typed_json_group_test.go
