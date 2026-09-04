#!/bin/sh
set -eu

gofmt -w token_bloom_filter.go token_bloom_filter_api_test.go hat/hatDataStructure/token_bloom_filter.go hat/hatDataStructure/token_bloom_filter_test.go hat/hatDataStructure/token_bloom_filter_benchmark_test.go
