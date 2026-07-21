#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
pause_benchmark=${ONLINE_COMPACTION_PAUSE_BENCH:-^BenchmarkMemoryCompactionReadPause10k$}
churn_benchmark=${ONLINE_COMPACTION_CHURN_BENCH:-^BenchmarkBigWins/(ChurnRetentionBaseline|ChurnRetentionCompacted)$}
keys=${BIG_WINS_KEYS:-100000}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/online-memory-compaction.txt"

mkdir -p "$artifact_dir"
{
	echo "# Online memory compaction pause"
	go test . \
		-run '^$' \
		-bench "$pause_benchmark" \
		-benchmem \
		-benchtime "$benchtime" \
		-count "$count"
	echo
	echo "# Delete-churn retention"
	HATRIE_BIG_WINS_KEYS="$keys" go test . \
		-run '^$' \
		-bench "$churn_benchmark" \
		-benchmem \
		-benchtime "$benchtime" \
		-count "$count"
} | tee "$output"
