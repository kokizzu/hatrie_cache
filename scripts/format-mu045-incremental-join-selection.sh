#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mu045_incremental_join_selection.go hat/hatSql/mu045_incremental_join_selection_test.go
