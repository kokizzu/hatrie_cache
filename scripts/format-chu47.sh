#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/dictionary.go hat/hatSql/sql_dictionary_test.go
