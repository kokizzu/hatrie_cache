#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/contracts.go hat/hatSql/columnar_layout_selection_test.go
