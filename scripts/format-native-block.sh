#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/native_block.go hat/hatSql/native_block_test.go
