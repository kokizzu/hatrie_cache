#!/usr/bin/env sh
set -eu

package=${PACKAGE:-.}
count=${COUNT:-1}
benchtime=${BENCHTIME:-}
default_bench='Benchmark(CommandWire(JSON|Protobuf)$|CommandJournal(Encode|Decode)(JSON|Binary)$|SnapshotFormat(JSON|Binary|GzipJSON|GzipBestJSON|GzipBinary|GzipBestBinary|Structured(JSON|Binary|GzipBestJSON|GzipBestBinary))$|LevelDB(Save(Materialized|MaterializedJSON|StructuredMaterialized|StructuredMaterializedJSON|ColdReferences|ColdReferencesStatsChanged)$|Load(Materialized|MaterializedJSON|StructuredMaterialized|StructuredMaterializedJSON|ColdReferences)$))'
bench=${SERIALIZATION_BENCH:-$default_bench}

set -- go test -run '^$' -bench "$bench" -benchmem -count "$count"
if [ -n "$benchtime" ]; then
	set -- "$@" -benchtime "$benchtime"
fi
set -- "$@" "$package"

printf '+'
for arg do
	printf ' %s' "$arg"
done
printf '\n'

exec "$@"
