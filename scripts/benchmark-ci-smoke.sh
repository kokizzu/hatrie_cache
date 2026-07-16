#!/usr/bin/env sh
set -eu

benchtime=${BENCH_CI_SMOKE_BENCHTIME:-${BENCHTIME:-5x}}
count=${BENCH_CI_SMOKE_COUNT:-${COUNT:-1}}
command_bench=${BENCH_CI_SMOKE_COMMAND_BENCH:-^BenchmarkCommandFeature/(StringGet|ReservoirSampleAdd)$}
transport_bench=${BENCH_CI_SMOKE_TRANSPORT_BENCH:-^BenchmarkCommandTransportFeature/InProcess/(StringSet|StringGet)$}
serialization_bench=${BENCH_CI_SMOKE_SERIALIZATION_BENCH:-Benchmark(CommandWireJSON|CommandWireProtobuf)$}

printf 'Benchmark CI smoke: benchtime=%s count=%s\n\n' "$benchtime" "$count"

printf '== Command feature benchmarks ==\n'
HATRIE_BENCH="$command_bench" BENCHTIME="$benchtime" COUNT="$count" ./scripts/benchmark-hatrie-command-features.sh

printf '\n== Transport feature benchmarks ==\n'
HATRIE_TRANSPORT_BENCH="$transport_bench" BENCHTIME="$benchtime" COUNT="$count" ./scripts/benchmark-hatrie-transport-features.sh

printf '\n== Serialization benchmarks ==\n'
SERIALIZATION_BENCH="$serialization_bench" BENCHTIME="$benchtime" COUNT="$count" ./scripts/benchmark-serialization.sh
