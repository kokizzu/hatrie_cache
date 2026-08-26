#!/usr/bin/env sh
set -eu

path_benchmark=${QUANTILE_BATCH_PATH_BENCH:-^BenchmarkQuantileSketch(Existing|Fresh)BatchCommandPath$}
alternating_benchmark=${QUANTILE_BATCH_ALTERNATING_BENCH:-^BenchmarkQuantileSketchCommandBatchAlternating$}
alternating_benchtime=${QUANTILE_BATCH_ALTERNATING_BENCHTIME:-200x}
control_benchmark=${QUANTILE_BATCH_CONTROL_BENCH:-^BenchmarkCommandFeature/(QuantileSketchAdd|MixedReadHeavy100|MixedWriteHeavy100)$}
control_benchtime=${QUANTILE_BATCH_CONTROL_BENCHTIME:-500ms}
baseline_binary=${QUANTILE_BATCH_BASELINE_BINARY:-}
candidate_binary=${QUANTILE_BATCH_CANDIDATE_BINARY:-}
benchtime=${BENCHTIME:-500ms}
count=${COUNT:-1}

go test ./hat/hatCache \
	-run '^$' \
	-bench="$path_benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1

go test ./hat/hatCache \
	-run '^$' \
	-bench="$alternating_benchmark" \
	-benchtime="$alternating_benchtime" \
	-count="$count" \
	-cpu=1

if [ -z "$baseline_binary" ] && [ -z "$candidate_binary" ]; then
	exit 0
fi
if [ ! -x "$baseline_binary" ] || [ ! -x "$candidate_binary" ]; then
	echo "QUANTILE_BATCH_BASELINE_BINARY and QUANTILE_BATCH_CANDIDATE_BINARY must both name executable Go test binaries" >&2
	exit 1
fi

iteration=1
while [ "$iteration" -le "$count" ]; do
	if [ $((iteration % 2)) -eq 1 ]; then
		first_label=baseline
		first_binary=$baseline_binary
		second_label=candidate
		second_binary=$candidate_binary
	else
		first_label=candidate
		first_binary=$candidate_binary
		second_label=baseline
		second_binary=$baseline_binary
	fi
	printf '%s\n' "quantile batch control iteration $iteration: $first_label"
	"$first_binary" \
		-test.run='^$' \
		-test.bench="$control_benchmark" \
		-test.benchmem \
		-test.benchtime="$control_benchtime" \
		-test.count=1 \
		-test.cpu=1
	printf '%s\n' "quantile batch control iteration $iteration: $second_label"
	"$second_binary" \
		-test.run='^$' \
		-test.bench="$control_benchmark" \
		-test.benchmem \
		-test.benchtime="$control_benchtime" \
		-test.count=1 \
		-test.cpu=1
	iteration=$((iteration + 1))
done
