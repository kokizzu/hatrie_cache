#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${JOURNAL_WIRE_BENCH:-^BenchmarkCommandJournalTailWire10k$}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/journal-tail-wire.txt"

mkdir -p "$artifact_dir"
echo "Journal tail wire benchmark: bench=$benchmark entries=10000 benchtime=$benchtime count=$count"
echo
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"

echo
echo "Raw result: $output"
