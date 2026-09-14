#!/usr/bin/env bash
set -euo pipefail

printf 'CH-029 files:\n'
printf '%s\n' \
	CH029_DICTIONARY_BACKED_JOIN.md \
	hat/hatDictionary/sql_lookup.go \
	hat/hatSql/ch029_dictionary_join_test.go \
	hat/hatSql/ch029_dictionary_join_benchmark_test.go
printf 'CH-029 catalog rows:\n'
rg -n "CH-029|CH-29" README.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_BACKLOG.md BENCHMARK.md
printf 'CH-029 implementation symbols:\n'
rg -n "LookupSourceResolver|LOOKUP JOIN|NewSQLDictionaryLookupResolver" hat/hatSql/query.go hat/hatDictionary/sql_lookup.go
printf 'Working tree:\n'
git status --short
