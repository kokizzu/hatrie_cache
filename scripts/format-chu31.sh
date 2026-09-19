#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/remote_part_upload.go \
  hat/hatStorage/chu31_multipart_upload_test.go \
  hat/hatStorage/chu31_multipart_upload_benchmark_test.go
