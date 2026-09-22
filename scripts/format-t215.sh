#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/storage_space.go \
  hat/hatDataStructure/t215_storage_space_test.go
