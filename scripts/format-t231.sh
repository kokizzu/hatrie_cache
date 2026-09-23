#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/t231_space_after_replace_baseline_test.go \
  hat/hatDataStructure/t231_space_after_replace_test.go
