#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/count_min_sketch.go hat/hatCache/count_min_sketch_merge_test.go hat/hatCache/count_min_sketch_merge_import_test.go
