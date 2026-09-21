#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMerkle/part_checksum.go \
  ./hat/hatMerkle/part_manifest.go \
  ./hat/hatMerkle/c244_part_manifest_test.go \
  ./hat/hatMerkle/c244_part_manifest_benchmark_test.go \
  -run '^$' -bench '^BenchmarkC244PartManifest' -benchmem -count=5
