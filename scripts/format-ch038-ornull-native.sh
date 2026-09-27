#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m052c_native_dataflow.go hat/hatSql/ch038_ornull_native_test.go
