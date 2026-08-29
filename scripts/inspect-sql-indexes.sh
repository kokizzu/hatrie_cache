#!/usr/bin/env sh
set -eu

printf '%s\n' '== SQL index benchmark and implementation files =='
rg --files hat/hatCache
printf '%s\n' '== benchmark and type locations =='
rg -n -A35 -B10 'Benchmark.*SQL|type sqlJSONFieldIndex|type sqlJSONFieldIndexEntry|func \(ht \*HatTrie\) CreateSQLJSONFieldIndex' hat/hatCache
