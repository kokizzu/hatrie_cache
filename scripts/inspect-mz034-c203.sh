#!/usr/bin/env bash
set -euo pipefail

printf 'MZ-034 catalog row:\n'
rg -n 'MZ-034' ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md
printf 'Existing differential/incremental files:\n'
rg -l 'Differential|Incremental|multiplic|retract|negative|signed' hat/hatSql hat/hatDataStructure hat/hatPipeline --glob '*.go' --glob '!**/*_test.go'
printf 'Existing signed-state symbols:\n'
rg -n 'type .*Diff|type .*Weight|type .*Multiplicity|Signed|Multiplicity|Weight|retract|negative' hat/hatSql hat/hatDataStructure hat/hatPipeline --glob '*.go' --glob '!**/*_test.go'
printf 'Differential row contract:\n'
sed -n '1,100p' hat/hatSql/differential_rows.go
printf 'Generic differential operators:\n'
sed -n '1,240p' hat/hatSql/differential_operators.go
printf 'Differential difference:\n'
sed -n '1,220p' hat/hatSql/differential_difference.go
printf 'Differential intersect:\n'
sed -n '1,240p' hat/hatSql/differential_intersect.go
printf 'Differential group-by count/sum:\n'
sed -n '1,240p' hat/hatSql/differential_count_sum.go
printf 'Differential public functions:\n'
rg -n '^func .*Differential|^func .*Incremental' hat/hatSql --glob '*.go' --glob '!**/*_test.go'
printf 'Differential tests:\n'
rg -l 'Differential|Incremental' hat/hatSql --glob '*_test.go'
printf 'Differential benchmark names:\n'
rg -n '^func Benchmark' hat/hatSql/differential_operators_benchmark_test.go hat/hatSql/differential_difference_benchmark_test.go hat/hatSql/differential_intersect_benchmark_test.go
printf 'EXCEPT benchmark definition:\n'
sed -n '1,130p' hat/hatSql/differential_difference_benchmark_test.go
printf 'INTERSECT benchmark definition:\n'
sed -n '1,150p' hat/hatSql/differential_intersect_benchmark_test.go
printf 'MZ-034 documentation references:\n'
rg -n 'MZ-034|negative-diff|negative diff|signed retraction|multiplic' ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md README.md
printf 'Adopted matrix header:\n'
sed -n '1,22p' ADOPTED_QUERY_ENGINE_IDEAS.md
