#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline \
  -run '^$' \
  -bench 'BenchmarkMutationDependencyGraphReady' \
  -benchmem \
  -count=5
