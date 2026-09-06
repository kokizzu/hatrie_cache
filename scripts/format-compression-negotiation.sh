#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
gofmt -w hat/hatCodec/compression_negotiation.go hat/hatCodec/compression_negotiation_test.go
