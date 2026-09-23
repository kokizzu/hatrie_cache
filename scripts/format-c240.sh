#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatBackup/read_only_attachment.go hat/hatBackup/c240_read_only_attachment_test.go hat/hatBackup/c240_read_only_attachment_benchmark_test.go hat/hatCache/pebble_store.go hat/hatCache/c240_read_only_pebble_test.go
