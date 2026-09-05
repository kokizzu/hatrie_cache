#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCodec/compressed_blocks.go hat/hatCodec/compressed_blocks_test.go
