#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${SCALAR_BATCH_BENCH:-^BenchmarkBigWins/(NativeBatchStreamCommand|ScalarBatchStreamCommand)$}
operations=${BIG_WINS_OPS:-10000}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-5}
output="$artifact_dir/scalar-protobuf-batch.txt"

mkdir -p "$artifact_dir"
HATRIE_BIG_WINS_OPS="$operations" go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
