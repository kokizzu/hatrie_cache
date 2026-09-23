#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/space_mvcc.go \
	hat/hatDataStructure/space_transaction.go \
	hat/hatDataStructure/t233_space_mvcc_benchmark_test.go \
	hat/hatDataStructure/t233_space_mvcc_test.go
