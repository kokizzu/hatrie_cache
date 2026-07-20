#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${JOURNAL_APPLY_BENCH:-^BenchmarkJournalPullApplyBatch10K$}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/journal-pull-batch-apply.txt"

mkdir -p "$artifact_dir"
echo "Journal pull batch apply benchmark: bench=$benchmark records=10000 benchtime=$benchtime count=$count"
echo
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"

echo
echo "Raw result: $output"
