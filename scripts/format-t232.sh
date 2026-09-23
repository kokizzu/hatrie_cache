#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/lsm_table.go \
	hat/hatDataStructure/space_transaction.go \
	hat/hatDataStructure/t232_space_transaction_test.go \
	hat/hatDataStructure/t232_space_transaction_benchmark_test.go
