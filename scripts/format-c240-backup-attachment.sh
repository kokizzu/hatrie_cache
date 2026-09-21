#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/backup_attachment.go hat/hatCache/c240_backup_attachment_test.go hat/hatCache/c240_backup_attachment_baseline_benchmark_test.go
