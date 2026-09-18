#!/usr/bin/env bash
set -eu
go test ./hat/hatSchema -run '^$' -bench 'BenchmarkTR046SchemaDiscoveryJSON(Marshal|Unmarshal)$' -benchmem -benchtime=250ms -count=5
