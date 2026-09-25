#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/space_store.go hat/hatCache/tt012_space_store_test.go
