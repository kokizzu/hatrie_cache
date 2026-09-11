#!/usr/bin/env bash
set -euo pipefail

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
keys=${MZ017_RESTORE_KEYS:-100000}
partitions=${MZ017_RESTORE_PARTITIONS:-16}
cpus=${MZ017_RESTORE_CPUS:-1 2 4 8 16 32}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-3}
output="$artifact_dir/mz017-restore-workers.txt"

mkdir -p "$artifact_dir"
: > "$output"
printf 'MZ-017 restore workers: keys=%s partitions=%s cpus=%s benchtime=%s count=%s\n' "$keys" "$partitions" "$cpus" "$benchtime" "$count"
for cpu in $cpus; do
  printf '\n--- cpu=%s ---\n' "$cpu" | tee -a "$output"
  HATRIE_PARTITION_RESTORE_KEYS="$keys" \
  HATRIE_PARTITION_RESTORE_COUNT="$partitions" \
  go test ./hat/hatCache \
    -run '^$' \
    -bench '^BenchmarkLocalPartitionRestore100k$' \
    -benchmem \
    -benchtime "$benchtime" \
    -count "$count" \
    -cpu "$cpu" | tee -a "$output"
done

printf '\nRaw result: %s\n' "$output"
