#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/ch025_token_postings_index.go \
	hat/hatDataStructure/chu48_ranked_token_postings_index.go \
	hat/hatDataStructure/chu48_ranked_token_postings_index_benchmark_test.go \
	hat/hatDataStructure/chu48_ranked_token_postings_index_test.go
