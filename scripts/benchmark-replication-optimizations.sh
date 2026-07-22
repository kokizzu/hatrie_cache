#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
split_benchmark=${REPLICATION_SPLIT_BENCH:-BenchmarkSplitReplicationTaskGroupByMaxBytes}
sync_benchmark=${REPLICATION_SYNC_BENCH:-BenchmarkHTTPReplicatorSyncAllBatching/Batched10k}
digest_benchmark=${REPLICATION_DIGEST_BENCH:-BenchmarkReplicationDigestChangesDefaultWire}
iterator_benchmark=${REPLICATION_ITERATOR_BENCH:-BenchmarkReplicationDigest\(SourceIteratorModes\|FallbackCollectionModes\)}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output_name=${REPLICATION_OPTIMIZATION_OUTPUT:-replication-optimization.txt}
output="$artifact_dir/$output_name"

mkdir -p "$artifact_dir"
{
	go test . \
		-run '^$' \
		-bench "$split_benchmark" \
		-benchmem \
		-benchtime "$benchtime" \
		-count "$count"
	go test . \
		-run '^$' \
		-bench "$sync_benchmark" \
		-benchmem \
		-benchtime "$benchtime" \
		-count "$count"
	go test . \
		-run '^$' \
		-bench "$digest_benchmark" \
		-benchmem \
		-benchtime "$benchtime" \
		-count "$count"
	go test . \
		-run '^$' \
		-bench "$iterator_benchmark" \
		-benchmem \
		-benchtime "$benchtime" \
		-count "$count"
} | tee "$output"
