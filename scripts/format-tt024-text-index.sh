#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSchema/materialized.go \
  hat/hatSchema/text_index.go \
  hat/hatSchema/text_index_resolver.go \
  hat/hatSchema/tt024_text_proximity_index_test.go
