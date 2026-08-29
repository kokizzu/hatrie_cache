#!/usr/bin/env sh
set -eu

printf '%s\n' '== SQL package files =='
rg --files -g '*.go' -g '!**/*_test.go' hat/hatSql
rg --files -g 'sql_*.go' -g '!**/*_test.go' hat/hatCache

printf '%s\n' '== SQL execution and planning entry points =='
rg -n -m 200 '^func (ExecuteSQLQuery|executeSQL|parseSQL|planSQL|optimizeSQL|evalSQL|sqlMerge|sqlBuild|sqlResolve|sqlIndex|sqlColumnar|sqlSpill)' hat/hatSql/query.go hat/hatSql/result_cache.go hat/hatSql/prepared.go hat/hatSql/adaptive.go hat/hatSql/vector.go hat/hatCache/sql_query.go hat/hatCache/sql_transaction.go

printf '%s\n' '== SQL allocations, copies, conversions, and synchronization =='
rg -n -m 200 '(make\(|append\(|Clone|clone|Marshal|Unmarshal|Decode|Encode|Copy|copy\(|sync\.|Mutex|RWMutex|Pool|bytes\.New|strings\.Builder)' hat/hatSql/query.go hat/hatSql/result_cache.go hat/hatSql/prepared.go hat/hatCache/sql_query.go

printf '%s\n' '== SQL command, planner, and benchmark coverage =='
rg -n -m 200 '(BenchmarkSQL|Benchmark.*SQL|SELECT|INSERT|UPDATE|DELETE|JOIN|GROUP BY|ORDER BY|WHERE|LIMIT|OFFSET|EXPLAIN)' hat/hatSql/*benchmark_test.go hat/hatCache/sql_*benchmark_test.go

printf '%s\n' '== Existing SQL TODO and risk markers =='
rg -n -i -m 160 '(TODO|FIXME|HACK|unsupported|fallback|slow|expensive|allocation|contention|spill)' hat/hatSql/query.go hat/hatSql/result_cache.go hat/hatSql/prepared.go hat/hatSql/adaptive.go hat/hatCache/sql_query.go
