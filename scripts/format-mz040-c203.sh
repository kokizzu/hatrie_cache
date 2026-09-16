#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mz040_recursive_diagnostics.go hat/hatSql/mz040_recursive_diagnostics_test.go
