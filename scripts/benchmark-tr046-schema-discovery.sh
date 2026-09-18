#!/usr/bin/env bash
set -eu
go test ./hat/hatSchema -run '^$' -bench 'BenchmarkTR046SchemaDiscovery(BinaryMarshal|BinaryUnmarshal|JSONMarshal|JSONUnmarshal)$' -benchmem -benchtime=250ms -count=5
