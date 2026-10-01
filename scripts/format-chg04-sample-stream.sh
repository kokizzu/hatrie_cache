#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch042_sample_stream.go hat/hatSql/chg04_sample_stream_test.go
