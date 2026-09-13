#!/bin/sh
set -eu

sed -n '1,230p' hat/hatPipeline/c202_partitioned_async_batcher_benchmark_test.go
sed -n '1,250p' hat/hatPipeline/c202_partitioned_async_batcher_test.go
