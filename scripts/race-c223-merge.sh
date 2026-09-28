#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure/quantile.go ./hat/hatDataStructure/quantile_merge.go ./hat/hatDataStructure/c223_merge_test.go
