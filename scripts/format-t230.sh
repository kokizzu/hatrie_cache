#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/t230_space_on_replace_baseline_test.go \
  hat/hatDataStructure/t230_space_on_replace_test.go
