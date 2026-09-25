#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch048_dictionary_predicate_test.go hat/hatSql/columnar_dictionary_predicate.go hat/hatSql/query.go
