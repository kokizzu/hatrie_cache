#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

printf '%s\n' '== approximate data structure APIs =='
printf '%s\n' '== approximate structure files =='
rg -l 'Top|top|Reservoir|reservoir' "$root/hat/hatDataStructure" --glob '!**/*_test.go'
rg -n -A 20 -B 4 'type (TopK|ReservoirSample)|func New(Default)?(HyperLogLog|QuantileSketch|TopK|ReservoirSample)|func \(.*(HyperLogLog|QuantileSketch|TopK|ReservoirSample).*\) (Add|Count|Estimate|Items|Values|Sample)' "$root/hat/hatDataStructure" --glob '!**/*_test.go'
printf '%s\n' '== SQL aggregate evaluation =='
sed -n '10335,10420p' "$root/hat/hatSql/query.go"
rg -n -A 100 -B 12 'func sqlExprHasAggregate|func sqlQueryHasAggregate' "$root/hat/hatSql/query.go"
printf '%s\n' '== SQL query API and aggregate tests =='
go doc hatrie_cache/hat/hatSql.ExecuteSQLQuery
go doc hatrie_cache/hat/hatSql.SourceResolver
