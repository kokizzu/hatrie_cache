#!/bin/sh
set -eu
gofmt -w hat/hatDataStructure/tuple_format.go hat/hatDataStructure/tr020_versioned_tuple.go hat/hatDataStructure/tr020_versioned_tuple_test.go hat/hatDataStructure/tr020_versioned_tuple_benchmark_test.go
