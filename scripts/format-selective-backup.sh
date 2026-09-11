#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatBackup/model.go hat/hatCache/backup_bundle.go hat/hatCache/backup_selective_test.go hat/hatCache/backup_selective_benchmark_test.go
