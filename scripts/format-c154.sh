#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSchema/rolling_schema.go hat/hatSchema/rolling_schema_test.go
