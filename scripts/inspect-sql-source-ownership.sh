#!/usr/bin/env sh
set -eu

rg -n -m 8 '^func .*GetBytesChecked' hat/hatCache
rg -n -m 8 '^func .*LockSQLSnapshot' hat/hatCache
rg -n -m 8 '^func .*ResolveSQLColumnarSource' hat/hatCache
rg -n -m 80 'SQLSnapshotLocker|LockSQLSnapshot|GetBytesChecked|ResolveSQLColumnarSource' hat/hatSql hat/hatCache/sql_query.go
rg -n -A 90 -B 20 '^func \(ht \*HatTrie\) GetBytesChecked' hat/hatCache/main.go
rg -n -A 100 -B 20 '^func \(ht \*HatTrie\) ResolveSQLColumnarSource' hat/hatCache/sql_query.go
rg -n -A 70 -B 20 '^func \(ht \*HatTrie\) LockSQLSnapshot' hat/hatCache/sql_query.go
rg -n -A 70 -B 20 'if locker, ok := resolver\.\(SQLSnapshotLocker\)' hat/hatSql/query.go
rg -n -A 180 -B 20 '^func sqlJSONColumnarBatch' hat/hatCache/sql_query.go
rg -n -A 120 -B 20 -m 1 '^func TestSQLColumnar' hat/hatCache/sql_columnar_scan_test.go
rg -n -A 100 -B 20 '^func BenchmarkSQLColumnarScan' hat/hatCache/sql_columnar_scan_benchmark_test.go
rg -n -m 80 '^func \(ht \*HatTrie\) .*Bytes' hat/hatCache/main.go
rg -n -m 40 'DiskBytesThreshold' hat/hatCache
