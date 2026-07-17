#!/usr/bin/env sh
set -eu

host=${REDIS_HOST:-127.0.0.1}
port=${REDIS_PORT:-6379}
requests=${REDIS_REQUESTS:-100000}
clients=${REDIS_CLIENTS:-1}
keyspace=${REDIS_KEYSPACE:-10000}
pipeline=${REDIS_PIPELINE:-16}
mixed_profile_ops=${REDIS_MIXED_PROFILE_OPS:-100}
start_docker=${REDIS_START_DOCKER:-0}
docker_image=${REDIS_DOCKER_IMAGE:-redis:7.0.4}
prefix=${REDIS_KEY_PREFIX:-hatriebench:$$}
artifact_dir=${BENCHMARK_ARTIFACT_DIR:-${REDIS_BENCHMARK_ARTIFACT_DIR:-}}
container=
raw_file=
rows_tsv=
memory_tsv=

if [ -n "$artifact_dir" ]; then
	mkdir -p "$artifact_dir"
	raw_file="$artifact_dir/redis-command-features.md"
	rows_tsv="$artifact_dir/redis-command-features.tsv"
	memory_tsv="$artifact_dir/redis-command-memory.tsv"
	: >"$raw_file"
	printf 'feature\tcommand\tthroughput_req_per_sec\tseconds_per_10k\n' >"$rows_tsv"
	printf 'metric\tvalue\tunit\n' >"$memory_tsv"
fi

emit() {
	printf "$@"
	if [ -n "$raw_file" ]; then
		printf "$@" >>"$raw_file"
	fi
}

cleanup() {
	if [ -n "$container" ]; then
		docker rm -f "$container" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT HUP INT TERM

redis_ready() {
	redis-cli -h "$host" -p "$port" ping >/dev/null 2>&1
}

if ! redis_ready; then
	case "$start_docker" in
		1|true|yes)
			container=$(docker run -d --rm -p "$port:6379" "$docker_image" redis-server --save "" --appendonly no)
			attempt=0
			while ! redis_ready; do
				attempt=$((attempt + 1))
				if [ "$attempt" -gt 100 ]; then
					echo "Redis did not become ready on $host:$port" >&2
					exit 1
				fi
				sleep 0.05
			done
			;;
		*)
			echo "Redis is not reachable at $host:$port. Set REDIS_START_DOCKER=1 to start a temporary container." >&2
			exit 2
			;;
	esac
fi

run_redis_benchmark() {
	feature=$1
	shift
	output=$(redis-benchmark -h "$host" -p "$port" -n "$requests" -c "$clients" -r "$keyspace" --csv "$@" | tail -n 1)
	qps=$(printf '%s\n' "$output" | awk -F, '{ value = $2; gsub(/"/, "", value); print value }')
	seconds_10k=$(awk -v qps="$qps" 'BEGIN { if (qps <= 0) { print "0.000000"; } else { printf "%.6f", 10000 / qps } }')
	emit '| %s | `%s` | %.2f req/s | %s s |\n' "$feature" "$*" "$qps" "$seconds_10k"
	if [ -n "$rows_tsv" ]; then
		printf '%s\t%s\t%.2f\t%s\n' "$feature" "$*" "$qps" "$seconds_10k" >>"$rows_tsv"
	fi
}

run_redis_pipeline_benchmark() {
	feature=$1
	shift
	output=$(redis-benchmark -h "$host" -p "$port" -n "$requests" -c "$clients" -r "$keyspace" -P "$pipeline" --csv "$@" | tail -n 1)
	qps=$(printf '%s\n' "$output" | awk -F, '{ value = $2; gsub(/"/, "", value); print value }')
	seconds_10k=$(awk -v qps="$qps" 'BEGIN { if (qps <= 0) { print "0.000000"; } else { printf "%.6f", 10000 / qps } }')
	command="-P $pipeline $*"
	emit '| %s | `%s` | %.2f req/s | %s s |\n' "$feature" "$command" "$qps" "$seconds_10k"
	if [ -n "$rows_tsv" ]; then
		printf '%s\t%s\t%.2f\t%s\n' "$feature" "$command" "$qps" "$seconds_10k" >>"$rows_tsv"
	fi
}

run_redis_lua_profile_benchmark() {
	feature=$1
	ops_per_cycle=$2
	script=$3
	output=$(redis-benchmark -h "$host" -p "$port" -n "$requests" -c "$clients" --csv EVAL "$script" 0 "$prefix" | tail -n 1)
	qps=$(printf '%s\n' "$output" | awk -F, '{ value = $2; gsub(/"/, "", value); print value }')
	seconds_10k=$(awk -v qps="$qps" -v ops="$ops_per_cycle" 'BEGIN { if (qps <= 0 || ops <= 0) { print "0.000000"; } else { printf "%.6f", 10000 / (qps * ops) } }')
	command="EVAL ${ops_per_cycle}op profile"
	emit '| %s | `%s` | %.2f req/s | %s s |\n' "$feature" "$command" "$qps" "$seconds_10k"
	if [ -n "$rows_tsv" ]; then
		printf '%s\t%s\t%.2f\t%s\n' "$feature" "$command" "$qps" "$seconds_10k" >>"$rows_tsv"
	fi
}

prime() {
	redis-benchmark -h "$host" -p "$port" -n "$1" -c "$clients" -r "$keyspace" "$2" "$3" "$4" >/dev/null
}

prime "$keyspace" SET "$prefix:string:__rand_int__" value
prime "$keyspace" LPUSH "$prefix:list:pop" __rand_int__
prime "$keyspace" SADD "$prefix:set" __rand_int__
redis-benchmark -h "$host" -p "$port" -n "$keyspace" -c "$clients" -r "$keyspace" ZADD "$prefix:zset:pop" __rand_int__ "member:__rand_int__" >/dev/null
redis-cli -h "$host" -p "$port" HSET "$prefix:hash" field value >/dev/null
redis-cli -h "$host" -p "$port" PFADD "$prefix:hll" value >/dev/null
redis-cli -h "$host" -p "$port" SETBIT "$prefix:bitmap" 65543 1 >/dev/null
redis-cli -h "$host" -p "$port" SET "$prefix:ttl" value >/dev/null
mixed_idx=1
while [ "$mixed_idx" -le "$mixed_profile_ops" ]; do
	redis-cli -h "$host" -p "$port" SET "$prefix:mixed:string:$mixed_idx" value >/dev/null
	mixed_idx=$((mixed_idx + 1))
done
redis-cli -h "$host" -p "$port" SET "$prefix:mixed:counter" 0 >/dev/null

emit 'Redis benchmark: host=%s port=%s requests=%s clients=%s keyspace=%s pipeline=%s mixed_profile_ops=%s\n\n' "$host" "$port" "$requests" "$clients" "$keyspace" "$pipeline" "$mixed_profile_ops"
emit '| Feature family | Redis command | Throughput | Seconds / 10k ops |\n'
emit '| --- | --- | ---: | ---: |\n'
run_redis_benchmark 'String write' SET "$prefix:string" value
run_redis_pipeline_benchmark 'Pipelined string write' SET "$prefix:pipeline:string" value
read_heavy_lua='local prefix = ARGV[1]; for i = 1, 90 do redis.call("GET", prefix .. ":mixed:string:" .. i); end; for i = 1, 5 do redis.call("SET", prefix .. ":mixed:string:" .. i, "value"); end; for i = 1, 4 do redis.call("EXISTS", prefix .. ":mixed:string:" .. i); end; redis.call("INCR", prefix .. ":mixed:counter"); return 1'
write_heavy_lua='local prefix = ARGV[1]; for i = 1, 40 do redis.call("SET", prefix .. ":mixed:string:" .. i, "value"); end; for i = 1, 30 do redis.call("EXPIRE", prefix .. ":mixed:string:" .. i, 3600); end; for i = 1, 20 do redis.call("GET", prefix .. ":mixed:string:" .. i); end; for i = 1, 10 do redis.call("INCR", prefix .. ":mixed:counter"); end; return 1'
run_redis_lua_profile_benchmark 'Mixed read-heavy profile' "$mixed_profile_ops" "$read_heavy_lua"
run_redis_lua_profile_benchmark 'Mixed write-heavy profile' "$mixed_profile_ops" "$write_heavy_lua"
run_redis_benchmark 'String read' GET "$prefix:string:__rand_int__"
run_redis_benchmark 'Integer counter' INCR "$prefix:counter"
run_redis_benchmark 'TTL update' EXPIRE "$prefix:ttl" 3600
run_redis_benchmark 'Hash/map write' HSET "$prefix:hash" field value
run_redis_benchmark 'Hash/map read' HGET "$prefix:hash" field
run_redis_benchmark 'List push' LPUSH "$prefix:list" value
run_redis_benchmark 'List pop' RPOP "$prefix:list:pop"
run_redis_benchmark 'Set add' SADD "$prefix:set" value
run_redis_benchmark 'Set membership' SISMEMBER "$prefix:set" value
run_redis_benchmark 'Sorted-set add' ZADD "$prefix:zset" 10 value
run_redis_benchmark 'Sorted-set pop' ZPOPMIN "$prefix:zset:pop"
run_redis_benchmark 'HyperLogLog add' PFADD "$prefix:hll" value
run_redis_benchmark 'HyperLogLog count' PFCOUNT "$prefix:hll"
run_redis_benchmark 'Bitmap add' SETBIT "$prefix:bitmap" 65543 1
run_redis_benchmark 'Bitmap lookup' GETBIT "$prefix:bitmap" 65543
run_redis_benchmark 'Replication dump' DUMP "$prefix:string"

memory_field() {
	redis-cli -h "$host" -p "$port" INFO memory | tr -d '\r' | awk -F: -v key="$1" '$1 == key { print $2 }'
}

used_memory=$(memory_field used_memory)
used_memory_rss=$(memory_field used_memory_rss)
used_memory_peak=$(memory_field used_memory_peak)

emit '\nMemory summary:\n\n'
emit '| Metric | Value |\n'
emit '| --- | ---: |\n'
emit '| used_memory | %s B |\n' "${used_memory:-unknown}"
emit '| used_memory_rss | %s B |\n' "${used_memory_rss:-unknown}"
emit '| used_memory_peak | %s B |\n' "${used_memory_peak:-unknown}"
if [ -n "$memory_tsv" ]; then
	printf 'used_memory\t%s\tB\n' "${used_memory:-unknown}" >>"$memory_tsv"
	printf 'used_memory_rss\t%s\tB\n' "${used_memory_rss:-unknown}" >>"$memory_tsv"
	printf 'used_memory_peak\t%s\tB\n' "${used_memory_peak:-unknown}" >>"$memory_tsv"
fi
