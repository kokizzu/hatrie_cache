#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
gofmt -w hat/hatSql/granule_sizing.go hat/hatSql/granule_sizing_test.go
