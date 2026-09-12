#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/sql_function.go \
  hat/hatCache/sql_wazero.go \
  hat/hatCache/sql_wasm_limits_test.go
