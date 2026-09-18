#!/usr/bin/env bash
set -eu
go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkCH020(SequentialReplicaReads|ParallelReplicaReads|SequentialNoop|ParallelNoop)$' -benchmem -benchtime=200ms -count=5
