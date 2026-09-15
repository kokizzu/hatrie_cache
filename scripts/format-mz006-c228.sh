#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatBackup/object_store_gc.go \
  hat/hatBackup/mz006_object_store_gc_test.go \
  hat/hatBackup/mz006_object_store_gc_benchmark_test.go
