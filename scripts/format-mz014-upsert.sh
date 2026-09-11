#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/upsert_batch.go \
	hat/hatDataStructure/upsert_batch_test.go \
	hat/hatDataStructure/upsert_batch_benchmark_test.go
