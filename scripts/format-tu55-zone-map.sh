#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/zone_map_index.go \
  hat/hatDataStructure/tu55_zone_map_test.go \
  hat/hatDataStructure/tu55_zone_map_benchmark_test.go
