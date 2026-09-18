#!/bin/sh
set -eu
gofmt -w hat/hatPipeline/tr049_queue_partition_ownership.go hat/hatPipeline/tr049_queue_partition_ownership_test.go hat/hatPipeline/tr049_queue_partition_ownership_benchmark_test.go
