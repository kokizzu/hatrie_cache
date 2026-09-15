#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatReplication/parallel_replica_read.go \
	hat/hatReplication/parallel_replica_read_fastpath_test.go \
	hat/hatReplication/parallel_replica_read_fastpath_benchmark_test.go
