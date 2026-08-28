#!/usr/bin/env sh
set -eu

for file in \
	hat/hatSql/temporal_analytics.go \
	hat/hatSql/time_series.go \
	hat/hatSql/approx_aggregate.go \
	hat/hatSql/query.go
do
	printf '\n=== %s exported types and functions ===\n' "$file"
	grep -n -E '^(type|func) [A-Z]' "$file" || true
done

printf '\n=== parser and executor integration markers ===\n'
grep -n -E 'AS OF|WATERMARK|SESSION|INTERVAL|ROLLUP|GEO|GRAPH|MATCH_RECOGNIZE|APPROX' hat/hatSql/query.go || true

printf '\n=== source parser markers ===\n'
grep -n -E 'parse.*(From|Source)|from\.kind|type sqlSource' hat/hatSql/query.go || true

printf '\n=== source model ===\n'
sed -n '4525,4565p' hat/hatSql/query.go
printf '\n=== source parser ===\n'
sed -n '5031,5190p' hat/hatSql/query.go
printf '\n=== source execution ===\n'
sed -n '10875,10935p' hat/hatSql/query.go

printf '\n=== temporal implementation and regression test ===\n'
sed -n '1,180p' hat/hatSql/temporal_analytics.go
sed -n '1,220p' hat/hatSql/temporal_analytics_test.go

printf '\n=== existing temporal commit script ===\n'
sed -n '1,220p' scripts/commit-temporal-analytics.sh

printf '\n=== external quality implementation and regression test ===\n'
sed -n '1,260p' hat/hatSql/external_quality.go
sed -n '1,260p' hat/hatSql/external_quality_test.go
