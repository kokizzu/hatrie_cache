#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
gofmt -w hat/hatDataStructure/nullable_bitmap.go hat/hatDataStructure/nullable_bitmap_test.go
