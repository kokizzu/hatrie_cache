#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/named_settings.go hat/hatSql/named_settings_test.go hat/hatSql/named_settings_benchmark_test.go
