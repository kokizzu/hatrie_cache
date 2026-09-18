#!/usr/bin/env bash
set -eu
go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkCH020Sequential(ReplicaReads|Noop)$' -benchmem -benchtime=200ms -count=5
