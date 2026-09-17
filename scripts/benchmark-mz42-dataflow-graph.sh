#!/bin/sh
set -eu

GOMAXPROCS=1 go test ./hat/hatPipeline -run '^$' -bench 'Benchmark(DataflowGraph.*|PipelineDescribe)$' -benchmem -count=5
