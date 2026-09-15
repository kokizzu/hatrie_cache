#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatBackup/catalog.go hat/hatBackup/catalog_test.go hat/hatBackup/catalog_benchmark_test.go
