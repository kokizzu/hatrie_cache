#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
hatrie_tsv=${HATRIE_BENCHMARK_TSV:-$artifact_dir/hatrie-command-features.tsv}
redis_tsv=${REDIS_BENCHMARK_TSV:-$artifact_dir/redis-command-features.tsv}
tarantool_tsv=${TARANTOOL_BENCHMARK_TSV:-$artifact_dir/tarantool-command-features.tsv}
out=${BENCHMARK_COMPARISON_MD:-$artifact_dir/command-feature-comparison.md}

# Expected TSV schemas include seconds_per_10k from the HAT-trie, Redis, and
# Tarantool benchmark artifact writers.

if [ ! -f "$hatrie_tsv" ]; then
	echo "missing HAT-trie TSV: $hatrie_tsv" >&2
	exit 1
fi
if [ ! -f "$redis_tsv" ]; then
	echo "missing Redis TSV: $redis_tsv" >&2
	exit 1
fi
if [ ! -f "$tarantool_tsv" ]; then
	echo "missing Tarantool TSV: $tarantool_tsv" >&2
	exit 1
fi

mkdir -p "$(dirname "$out")"

awk -F'\t' '
function trim(value) {
	gsub(/^[ \t]+|[ \t]+$/, "", value)
	return value
}
function seconds(value) {
	value = trim(value)
	gsub(/ s$/, "", value)
	return value + 0
}
function fmt_seconds(value) {
	if (value <= 0) {
		return "missing"
	}
	return sprintf("%.6f s", value)
}
function fmt_alloc(value) {
	if (value == "") {
		return "missing"
	}
	return value " B/op"
}
function speedup(base, hat) {
	if (base <= 0 || hat <= 0) {
		return "missing"
	}
	return sprintf("%.2fx", base / hat)
}
function sum_features(names, seconds_by_feature, command_by_feature, output_command,   parts, count_parts, idx, name, total, commands) {
	count_parts = split(names, parts, /\+/)
	total = 0
	commands = ""
	for (idx = 1; idx <= count_parts; idx++) {
		name = trim(parts[idx])
		if (!(name in seconds_by_feature)) {
			output_command[1] = "missing"
			return 0
		}
		total += seconds_by_feature[name]
		if (commands != "") {
			commands = commands " + "
		}
		commands = commands "`" command_by_feature[name] "`"
	}
	output_command[1] = commands
	return total
}
function add_mapping(feature, hatrie, redis, tarantool) {
	count++
	features[count] = feature
	hatrie_bench[count] = hatrie
	redis_features[count] = redis
	tarantool_features[count] = tarantool
}
FILENAME == ARGV[1] && FNR > 1 {
	hatrie_seconds[$1] = seconds($6)
	hatrie_allocs[$1] = $4
	next
}
FILENAME == ARGV[2] && FNR > 1 {
	redis_seconds[$1] = seconds($4)
	redis_commands[$1] = $2
	next
}
FILENAME == ARGV[3] && FNR > 1 {
	tarantool_seconds[$1] = seconds($3)
	tarantool_operations[$1] = $2
	next
}
END {
	add_mapping("String write", "BenchmarkCommandFeature/StringSet", "String write", "String write")
	add_mapping("Pipelined string write", "BenchmarkCommandFeature/PipelineBatch16", "Pipelined string write", "Pipelined string write")
	add_mapping("String read", "BenchmarkCommandFeature/StringGet", "String read", "String read")
	add_mapping("Integer counter", "BenchmarkCommandFeature/CounterInc", "Integer counter", "Integer counter")
	add_mapping("TTL update", "BenchmarkCommandFeature/TTLExpire", "TTL update", "TTL update")
	add_mapping("Map/hash write", "BenchmarkCommandFeature/MapPut", "Hash/map write", "Map/hash write")
	add_mapping("Map/hash read", "BenchmarkCommandFeature/MapPeek", "Hash/map read", "Map/hash read")
	add_mapping("List/deque push+pop", "BenchmarkCommandFeature/SlicePushPop", "List push+List pop", "List/deque push+pop")
	add_mapping("Set add+has", "BenchmarkCommandFeature/SetAddHas", "Set add+Set membership", "Set add+has")
	add_mapping("Priority queue push+pop", "BenchmarkCommandFeature/PriorityQueuePushPop", "Sorted-set add+Sorted-set pop", "Priority queue push+pop")
	add_mapping("Roaring bitmap add", "BenchmarkCommandFeature/RoaringAdd", "Bitmap add", "Roaring bitmap add approximation")
	add_mapping("Roaring bitmap lookup", "BenchmarkCommandFeature/RoaringHas", "Bitmap lookup", "Roaring bitmap lookup approximation")
	add_mapping("Sparse uint64 bitset add", "BenchmarkCommandFeature/SparseBitsetAdd", "Bitmap add", "Sparse bitset add approximation")
	add_mapping("Sparse uint64 bitset lookup", "BenchmarkCommandFeature/SparseBitsetHas", "Bitmap lookup", "Sparse bitset lookup approximation")
	add_mapping("Radix-tree put", "BenchmarkCommandFeature/RadixPut", "", "Radix-tree put approximation")
	add_mapping("Radix-tree prefix scan", "BenchmarkCommandFeature/RadixPrefix", "", "Radix-tree prefix scan approximation")
	add_mapping("HyperLogLog add", "BenchmarkCommandFeature/HyperLogLogAdd", "HyperLogLog add", "")
	add_mapping("HyperLogLog count", "BenchmarkCommandFeature/HyperLogLogCount", "HyperLogLog count", "")
	add_mapping("Replication dump", "BenchmarkCommandFeature/ReplicationDump", "Replication dump", "Replication dump")

	print "# Command Feature Benchmark Comparison"
	print ""
	print "Generated from TSV artifacts in the benchmark artifact directory."
	print ""
	print "## HAT-trie vs Tarantool"
	print ""
	print "| Feature family | HAT-trie benchmark | HAT-trie seconds / 10k | HAT alloc/op | Tarantool measured operation | Tarantool seconds / 10k | Tarantool/HAT speedup |"
	print "| --- | --- | ---: | ---: | --- | ---: | ---: |"
	for (idx = 1; idx <= count; idx++) {
		tfeature = tarantool_features[idx]
		if (tfeature == "") {
			continue
		}
		hsec = hatrie_seconds[hatrie_bench[idx]]
		tsec = tarantool_seconds[tfeature]
		operation = (tfeature in tarantool_operations) ? "`" tarantool_operations[tfeature] "`" : "missing"
		print "| " features[idx] " | `" hatrie_bench[idx] "` | " fmt_seconds(hsec) " | " fmt_alloc(hatrie_allocs[hatrie_bench[idx]]) " | " operation " | " fmt_seconds(tsec) " | " speedup(tsec, hsec) " |"
	}
	print ""
	print "## HAT-trie vs Redis"
	print ""
	print "| Feature family | HAT-trie benchmark | HAT-trie seconds / 10k | Redis measured command | Redis seconds / 10k | Redis/HAT speedup |"
	print "| --- | --- | ---: | --- | ---: | ---: |"
	for (idx = 1; idx <= count; idx++) {
		rfeature = redis_features[idx]
		if (rfeature == "") {
			continue
		}
		hsec = hatrie_seconds[hatrie_bench[idx]]
		command[1] = ""
		rsec = sum_features(rfeature, redis_seconds, redis_commands, command)
		print "| " features[idx] " | `" hatrie_bench[idx] "` | " fmt_seconds(hsec) " | " command[1] " | " fmt_seconds(rsec) " | " speedup(rsec, hsec) " |"
	}
}
' "$hatrie_tsv" "$redis_tsv" "$tarantool_tsv" >"$out"

cat "$out"
