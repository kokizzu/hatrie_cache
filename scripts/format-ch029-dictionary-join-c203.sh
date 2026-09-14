#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch029_dictionary_join_test.go hat/hatSql/ch029_dictionary_join_benchmark_test.go hat/hatSql/query.go hat/hatDictionary/ch029_dictionary_join_test.go hat/hatDictionary/sql_lookup.go
