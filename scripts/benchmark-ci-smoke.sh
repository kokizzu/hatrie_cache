#!/usr/bin/env sh
set -eu

benchtime=${BENCH_CI_SMOKE_BENCHTIME:-${BENCHTIME:-5x}}
count=${BENCH_CI_SMOKE_COUNT:-${COUNT:-1}}
command_bench=${BENCH_CI_SMOKE_COMMAND_BENCH:-^BenchmarkCommandFeature/(StringGet|ReservoirSampleAdd)$}
transport_bench=${BENCH_CI_SMOKE_TRANSPORT_BENCH:-^BenchmarkCommandTransportFeature/InProcess/(StringSet|StringGet)$}
serialization_bench=${BENCH_CI_SMOKE_SERIALIZATION_BENCH:-Benchmark(CommandWireJSON|CommandWireProtobuf)$}
check_thresholds=${BENCH_CI_SMOKE_CHECK_THRESHOLDS:-0}
max_command_ns=${BENCH_CI_SMOKE_MAX_COMMAND_NS_OP:-250000}
max_transport_ns=${BENCH_CI_SMOKE_MAX_TRANSPORT_NS_OP:-500000}
max_serialization_ns=${BENCH_CI_SMOKE_MAX_SERIALIZATION_NS_OP:-250000}
max_b_op=${BENCH_CI_SMOKE_MAX_B_OP:-1048576}
max_allocs_op=${BENCH_CI_SMOKE_MAX_ALLOCS_OP:-512}
artifact_dir=${BENCH_CI_SMOKE_ARTIFACT_DIR:-}
baseline_json=${BENCH_CI_SMOKE_BASELINE_JSON:-}
max_regression_pct=${BENCH_CI_SMOKE_MAX_REGRESSION_PCT:-20}
compare_memory=${BENCH_CI_SMOKE_COMPARE_MEMORY:-0}
run_id=${BENCH_CI_SMOKE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-bench-ci-smoke.XXXXXX")
command_output="$tmp_dir/command.txt"
transport_output="$tmp_dir/transport.txt"
serialization_output="$tmp_dir/serialization.txt"
rows_file="$tmp_dir/rows.tsv"
current_json="$tmp_dir/benchmark-ci-smoke.json"
current_md="$tmp_dir/benchmark-ci-smoke.md"

cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

bool_true() {
	case "$1" in
		1|true|TRUE|yes|YES|on|ON)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

run_capture() {
	output_file=$1
	shift
	if "$@" >"$output_file" 2>&1; then
		cat "$output_file"
	else
		status=$?
		cat "$output_file"
		exit "$status"
	fi
}

check_benchmark_file() {
	label=$1
	output_file=$2
	max_ns=$3
	awk \
		-v label="$label" \
		-v max_ns="$max_ns" \
		-v max_b_op="$max_b_op" \
		-v max_allocs_op="$max_allocs_op" '
function number(value) {
	gsub(/,/, "", value)
	return value + 0
}
/^Benchmark/ {
	seen = 1
	name = $1
	ns = -1
	bytes = -1
	allocs = -1
	for (i = 2; i <= NF; i++) {
		if ($i == "ns/op" && i > 1) {
			ns = number($(i - 1))
		}
		if ($i == "B/op" && i > 1) {
			bytes = number($(i - 1))
		}
		if ($i == "allocs/op" && i > 1) {
			allocs = number($(i - 1))
		}
	}
	if (max_ns > 0 && ns >= 0 && ns > max_ns) {
		printf("%s regression: %s %.0f ns/op > %.0f ns/op\n", label, name, ns, max_ns) > "/dev/stderr"
		failures++
	}
	if (max_b_op > 0 && bytes >= 0 && bytes > max_b_op) {
		printf("%s regression: %s %.0f B/op > %.0f B/op\n", label, name, bytes, max_b_op) > "/dev/stderr"
		failures++
	}
	if (max_allocs_op > 0 && allocs >= 0 && allocs > max_allocs_op) {
		printf("%s regression: %s %.0f allocs/op > %.0f allocs/op\n", label, name, allocs, max_allocs_op) > "/dev/stderr"
		failures++
	}
}
END {
	if (!seen) {
		printf("%s regression guard: no benchmark rows found\n", label) > "/dev/stderr"
		exit 1
	}
	if (failures > 0) {
		exit 1
	}
}
' "$output_file"
}

collect_benchmark_rows() {
	section=$1
	output_file=$2
	awk -v section="$section" '
function number(value) {
	gsub(/,/, "", value)
	return value + 0
}
/^Benchmark/ {
	name = $1
	ns = 0
	bytes = 0
	allocs = 0
	for (i = 2; i <= NF; i++) {
		if ($i == "ns/op" && i > 1) {
			ns = number($(i - 1))
		}
		if ($i == "B/op" && i > 1) {
			bytes = number($(i - 1))
		}
		if ($i == "allocs/op" && i > 1) {
			allocs = number($(i - 1))
		}
	}
	printf "%s\t%s\t%.0f\t%.0f\t%.0f\n", section, name, ns, bytes, allocs
}
' "$output_file"
}

write_json_artifact() {
	output_file=$1
	{
		printf '{\n'
		printf '  "schema": "hatrie-cache-benchmark-ci-smoke/v1",\n'
		printf '  "run_id": "%s",\n' "$run_id"
		printf '  "created_at": "%s",\n' "$created_at"
		printf '  "benchtime": "%s",\n' "$benchtime"
		printf '  "count": "%s",\n' "$count"
		printf '  "metadata": {\n'
		printf '    "command_bench": "%s",\n' "$(json_escape "$command_bench")"
		printf '    "transport_bench": "%s",\n' "$(json_escape "$transport_bench")"
		printf '    "serialization_bench": "%s"\n' "$(json_escape "$serialization_bench")"
		printf '  },\n'
		printf '  "results": [\n'
		awk -F '\t' '
function escape(value) {
	gsub(/\\/, "\\\\", value)
	gsub(/"/, "\\\"", value)
	return value
}
{
	if (NR > 1) {
		printf ",\n"
	}
	printf "    {\"section\":\"%s\",\"name\":\"%s\",\"ns_op\":%s,\"b_op\":%s,\"allocs_op\":%s}", escape($1), escape($2), $3, $4, $5
}
END {
	if (NR > 0) {
		printf "\n"
	}
}
' "$rows_file"
		printf '  ]\n'
		printf '}\n'
	} >"$output_file"
}

json_escape() {
	printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

write_markdown_artifact() {
	output_file=$1
	{
		printf '%s\n\n' '# Benchmark CI Smoke'
		printf '%s\n' "- Run ID: \`$run_id\`"
		printf '%s\n' "- Created at: \`$created_at\`"
		printf '%s\n' "- Benchtime: \`$benchtime\`"
		printf '%s\n\n' "- Count: \`$count\`"
		printf '%s\n' '| Section | Benchmark | ns/op | B/op | allocs/op |'
		printf '%s\n' '| --- | --- | ---: | ---: | ---: |'
		awk -F '\t' '{ printf "| %s | `%s` | %s | %s | %s |\n", $1, $2, $3, $4, $5 }' "$rows_file"
	} >"$output_file"
}

write_artifacts() {
	: >"$rows_file"
	collect_benchmark_rows "Command feature" "$command_output" >>"$rows_file"
	collect_benchmark_rows "Transport feature" "$transport_output" >>"$rows_file"
	collect_benchmark_rows "Serialization" "$serialization_output" >>"$rows_file"
	write_json_artifact "$current_json"
	write_markdown_artifact "$current_md"
	if [ -n "$artifact_dir" ]; then
		mkdir -p "$artifact_dir/raw"
		cp "$command_output" "$artifact_dir/raw/command.txt"
		cp "$transport_output" "$artifact_dir/raw/transport.txt"
		cp "$serialization_output" "$artifact_dir/raw/serialization.txt"
		cp "$rows_file" "$artifact_dir/benchmark-ci-smoke.tsv"
		cp "$current_json" "$artifact_dir/benchmark-ci-smoke.json"
		cp "$current_md" "$artifact_dir/benchmark-ci-smoke.md"
		cp "$current_json" "$artifact_dir/benchmark-ci-smoke-$run_id.json"
		cp "$current_md" "$artifact_dir/benchmark-ci-smoke-$run_id.md"
		printf '\nBenchmark artifacts written to %s\n' "$artifact_dir"
	fi
}

compare_baseline() {
	if [ -z "$baseline_json" ]; then
		return
	fi
	compare_memory_flag=false
	if bool_true "$compare_memory"; then
		compare_memory_flag=true
	fi
	printf '\n== Baseline comparison ==\n'
	go run ./cmd/hatrie-benchcmp \
		-current "$current_json" \
		-baseline "$baseline_json" \
		-max-regression-pct "$max_regression_pct" \
		-compare-memory="$compare_memory_flag"
}

printf 'Benchmark CI smoke: benchtime=%s count=%s\n\n' "$benchtime" "$count"

printf '== Command feature benchmarks ==\n'
run_capture "$command_output" env HATRIE_BENCH="$command_bench" BENCHTIME="$benchtime" COUNT="$count" ./scripts/benchmark-hatrie-command-features.sh

printf '\n== Transport feature benchmarks ==\n'
run_capture "$transport_output" env HATRIE_TRANSPORT_BENCH="$transport_bench" BENCHTIME="$benchtime" COUNT="$count" ./scripts/benchmark-hatrie-transport-features.sh

printf '\n== Serialization benchmarks ==\n'
run_capture "$serialization_output" env SERIALIZATION_BENCH="$serialization_bench" BENCHTIME="$benchtime" COUNT="$count" ./scripts/benchmark-serialization.sh

if bool_true "$check_thresholds"; then
	printf '\n== Regression guard ==\n'
	printf 'Max ns/op: command=%s transport=%s serialization=%s; max B/op=%s; max allocs/op=%s\n' "$max_command_ns" "$max_transport_ns" "$max_serialization_ns" "$max_b_op" "$max_allocs_op"
	threshold_status=0
	check_benchmark_file "Command feature" "$command_output" "$max_command_ns" || threshold_status=1
	check_benchmark_file "Transport feature" "$transport_output" "$max_transport_ns" || threshold_status=1
	check_benchmark_file "Serialization" "$serialization_output" "$max_serialization_ns" || threshold_status=1
	if [ "$threshold_status" -ne 0 ]; then
		echo "Benchmark CI smoke regression guard failed" >&2
		exit 1
	fi
	echo "Benchmark CI smoke regression guard passed"
fi

if [ -n "$artifact_dir" ] || [ -n "$baseline_json" ]; then
	write_artifacts
	compare_baseline
fi
