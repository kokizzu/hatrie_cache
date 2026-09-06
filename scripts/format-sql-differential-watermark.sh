#!/usr/bin/env bash
set -euo pipefail

gofmt -w ./hat/hatSql/differential_watermark.go ./hat/hatSql/differential_watermark_test.go
