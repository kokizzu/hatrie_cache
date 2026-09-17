#!/bin/sh
set -eu

GOMAXPROCS=1 go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMU01ConnectorRegistry(StatusSnapshot|StateSnapshot|BinarySnapshot|BinaryCodec|JSONCodec|BinaryDecode|JSONDecode)$' -benchmem -cpu=1 -count=5
