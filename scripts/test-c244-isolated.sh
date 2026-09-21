#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMerkle/part_checksum.go \
  ./hat/hatMerkle/part_manifest.go \
  ./hat/hatMerkle/c244_part_manifest_test.go
