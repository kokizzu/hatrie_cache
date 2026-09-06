#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
gofmt -w hat/hatCodec/bitpacked_numeric.go hat/hatCodec/bitpacked_numeric_test.go
