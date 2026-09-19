#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/remote_part_publication.go \
  hat/hatStorage/chu33_remote_part_publication_test.go \
  hat/hatStorage/chu33_remote_part_publication_benchmark_test.go
