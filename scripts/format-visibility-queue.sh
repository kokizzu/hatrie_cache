#!/bin/sh
set -eu
exec gofmt -w hat/hatDataStructure/visibility_queue.go hat/hatDataStructure/visibility_queue_test.go hat/hatDataStructure/visibility_queue_benchmark_test.go
