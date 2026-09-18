#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz036_dataflow_placement.go \
  hat/hatPipeline/mz036_dataflow_placement_test.go
